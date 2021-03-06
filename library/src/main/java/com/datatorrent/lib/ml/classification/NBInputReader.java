package com.datatorrent.lib.ml.classification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This is the Naive Bayes Input Reader Operator.
 * This will read the ARFF format files and pass on 
 * the attributes in the input data in a String array.
 * 
 * TODO 
 * This assumes that the input file is a plain csv file 
 * (i.e. it ignores the @header part of the ARFF files and just considers the @data portion which is just plain csv)
 * Also assumes that the class is present as the last attribute in the csv
 * @author 
 *
 */
public class NBInputReader extends BaseOperator{

  private static final Logger LOG = LoggerFactory.getLogger(NBInputReader.class);

  private String[] attributes;
  private NBConfig nbc = null;
  private boolean trainingPhase = true;
  private boolean evaluationPhase = false;

  public NBInputReader(){
  
  }

  public NBInputReader(NBConfig nbc){
    this.nbc = nbc;
    if(nbc.isKFoldPartition()){
      trainingPhase = true;
      evaluationPhase = false;
    }
  }

  /**
   * Output port that emits value of the fields.
   * Output data type can be configured in the implementation of this operator.
   */
  public final transient DefaultOutputPort<MapEntry<Integer,String[]>> outForEvaluation = new DefaultOutputPort<MapEntry<Integer,String[]>>();
  public final transient DefaultOutputPort<MapEntry<Integer,String[]>> outForTraining = new DefaultOutputPort<MapEntry<Integer,String[]>>();
  public final transient DefaultOutputPort<Boolean> controlOut = new DefaultOutputPort<Boolean>();

  public final transient DefaultInputPort<Boolean> controlIn = new DefaultInputPort<Boolean>(){

    @Override
    public void process(Boolean b)
    {
      if(b.booleanValue()){
        if(nbc.isKFoldPartition() && trainingPhase){
          trainingPhase = false;
          controlOut.emit(true);
//          outForTraining.emit(new MapEntry<Integer, String[]>(-1, null));
        }        
      }
    }
    
  };

  
  /**
   * Input port receives tuples as String 
   */
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>(){

    @Override
    public void process(String tuple) {

//      if(tuple.trim().equals("@FILE_END")){
//        if(nbc.isKFoldPartition() && trainingPhase){
//          trainingPhase = false;
//          outForTraining.emit(new MapEntry<Integer, String[]>(-1, null));
//          return;
//        }
//      }
      
      if(tuple.trim().startsWith("@") || tuple.trim().startsWith("%") || tuple.trim().length() == 0){
        return;
      }
      attributes = parseAsCsv(tuple);
      if(nbc.isKFoldPartition()){
        int fold = Math.abs(tuple.hashCode())%nbc.getNumFolds();
        MapEntry<Integer, String[]> partitionTuple = new MapEntry<Integer, String[]>(fold, attributes);
        if(trainingPhase)
          outForTraining.emit(partitionTuple);
        if(evaluationPhase)
          outForEvaluation.emit(partitionTuple);
      }
      if(!nbc.isKFoldPartition() && nbc.isOnlyTrain()){
        MapEntry<Integer, String[]> partitionTuple = new MapEntry<Integer, String[]>(-1, attributes);
        outForTraining.emit(partitionTuple);
      }
      if(nbc.isOnlyEvaluate()){
        MapEntry<Integer, String[]> partitionTuple = new MapEntry<Integer, String[]>(-1, attributes);
        outForEvaluation.emit(partitionTuple);
      }
    }
  };

  /**
   * Parses the String as a csv tuple
   * @param tuple
   * @return Array of String - listing the attributes
   */
  public static String[] parseAsCsv(String tuple){
    String[] attrs = tuple.trim().split(",");
    String[] retVal = new String[attrs.length];
    for(int i=0;i<attrs.length;i++){
      retVal[i] = attrs[i];
    }
    return retVal;
  }

  @Override
  public void endWindow() {
    if(!trainingPhase){
      evaluationPhase = true;
      //donePort.emit(true);
    }
  }
}
