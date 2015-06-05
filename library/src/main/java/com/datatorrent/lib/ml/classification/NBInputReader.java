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

	String[] attributes;
	NBConfig nbc = null;
	boolean trainingPhase = true;
	boolean evaluationPhase = false;

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
	public final transient DefaultOutputPort<Boolean> donePort = new DefaultOutputPort<Boolean>();

	/**
	 * Input port receives tuples as String 
	 */
	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>(){

		@Override
		public void process(String tuple) {
			if(nbc.isKFoldPartition()){ // Both Training and Evaluation Phases needed
				if(trainingPhase){
					if(tuple.trim().equals("@TRAINING_END")){
						trainingPhase = false;
						outForTraining.emit(new MapEntry<Integer, String[]>(-1, null));
						return;
					}
					if(tuple.trim().startsWith("@") || tuple.trim().startsWith("%") || tuple.trim().length() == 0){
						//LOG.info("Metadata String. Ignoring for now.");
						return;
					}
					attributes = parseAsCsv(tuple);
					int fold = Math.abs(tuple.hashCode())%nbc.getNumFolds();
					MapEntry<Integer, String[]> partitionTuple = new MapEntry<Integer, String[]>(fold, attributes);
					outForTraining.emit(partitionTuple);
				}
				else if(evaluationPhase){
					if(tuple.trim().startsWith("@") || tuple.trim().startsWith("%") || tuple.trim().length() == 0){
						//LOG.info("Metadata String. Ignoring for now.");
						return;
					}
					attributes = parseAsCsv(tuple);
					int fold = Math.abs(tuple.hashCode())%nbc.getNumFolds();
					MapEntry<Integer, String[]> partitionTuple = new MapEntry<Integer, String[]>(fold, attributes);
					outForEvaluation.emit(partitionTuple);
				}
			}
			if(nbc.isOnlyTrain() && !nbc.isKFoldPartition()){ // Only Training. Send on Training port
				if(tuple.trim().startsWith("@") || tuple.trim().startsWith("%") || tuple.trim().length() == 0){
					//LOG.info("Metadata String. Ignoring for now.");
					return;
				}
				attributes = parseAsCsv(tuple);
				MapEntry<Integer, String[]> partitionTuple = new MapEntry<Integer, String[]>(-1, attributes);
				outForTraining.emit(partitionTuple);				
			}
			if(nbc.isOnlyEvaluate()){ // Only Evaluation. Send on Evaluation port
				if(tuple.trim().startsWith("@") || tuple.trim().startsWith("%") || tuple.trim().length() == 0){
					//LOG.info("Metadata String. Ignoring for now.");
					return;
				}
				attributes = parseAsCsv(tuple);
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
