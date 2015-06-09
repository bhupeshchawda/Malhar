package com.datatorrent.lib.ml.classification;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

/**
 * This is the Naive Bayes Evaluation Operator.
 * This has two flows:
 * 1. Test data evaluation - In this case, this simply takes the input tuple and classifies it against the input model. The predicted class along with the accuracy is emitted at the output port.
 * 2. k-fold cross validation - In this case, the input tuple i is evaluated against the ith model (among the k models built). The accuracy is measured and batched accuracies for each model is emitted per window.
 * 
 * @author 
 *
 */
public class NBEvaluator extends BaseOperator{

  private static final Logger LOG = LoggerFactory.getLogger(NBEvaluator.class);

  NBModelStorage m;
  double correct = 0.0;
  double total = 0.0;

  NBConfig nbc = null;
  boolean initializedModels = false;
  boolean changeInWindow = false;
  NBModelStorage[] kFoldModels;

  int[] featureValueSizes;
  int[][] kFeatureValueSizes;

  int folds = 0;
  long[] kFoldCorrect;
  long[] kFoldTotal;

  public NBEvaluator(){
  }

  public NBEvaluator(NBConfig nbc){
    this.nbc = nbc;
    initializedModels = false;
    if(nbc.isKFoldPartition()){
      folds = nbc.getNumFolds();
      kFoldModels = new NBModelStorage[folds];
      kFeatureValueSizes = new int[folds][nbc.getNumAttributes()];
      kFoldCorrect = new long[folds];
      kFoldTotal = new long[folds];
    }
  }

  /**
   * Output port that emits the predicted class along with the cumulative accuracy
   */
  public final transient DefaultOutputPort<String> outToWriter = new DefaultOutputPort<String>();

  /**
   * Output port that emits statistics per window. 
   * TODO: Implement per window statistics.
   */
  public final transient DefaultOutputPort<String> stats = new DefaultOutputPort<String>();

  public final transient DefaultInputPort<MapEntry<Integer,NBModelStorage>> inKFoldModels = 
      new DefaultInputPort<MapEntry<Integer,NBModelStorage>>(){

    @Override
    public void process(MapEntry<Integer, NBModelStorage> foldModel)
    {
      int fold = foldModel.getK();
      kFoldModels[fold] = foldModel.getV();
      kFeatureValueSizes[fold] = kFoldModels[fold].getFeatureValueSize();
    }
  };
  /**
   * Input port for k-fold cross validation flow. This is for identifying the accuracy of the Naive Bayes model.
   * This port receives an entry of the form <Integer, String[]>. 
   * The key in the entry points to the "fold" of the input data. 
   * In other words, it gives the model against which this input tuple (pointed to by the value) needs to be evaluated.
   */
  public final transient DefaultInputPort<MapEntry<Integer,String[]>> inForEvaluation = 
      new DefaultInputPort<MapEntry<Integer,String[]>>(){

    @Override
    public void process(MapEntry<Integer, String[]> testInstance) {

      if(testInstance.getK().equals(-1)){
        return;
      }

      if(!initializedModels){
        initModels();
        initializedModels = true;
      }
      //      LOG.info("Tuple received: {}",testInstance.getK()+Arrays.toString(testInstance.getV()));

      if(nbc.isKFoldPartition()){
        int fold = testInstance.getK();
        String predictedClass = kFoldModels[fold].evaluate(testInstance.getV(), kFeatureValueSizes[fold]);
        String originalClass = testInstance.getV()[testInstance.getV().length-1];
        //        LOG.info("K Feature Value Sizes: {}",Arrays.toString(kFeatureValueSizes[fold]));
        //        LOG.info("Predicted class = {} Actual = {}", predictedClass, originalClass);
        if(predictedClass.trim().equalsIgnoreCase(originalClass.trim())){
          kFoldCorrect[fold] += 1;
        }
        kFoldTotal[fold] += 1;
        changeInWindow = true;
      }
      if(nbc.isOnlyEvaluate()) { // Plain evaluation. 
        // TODO Assumes that the input also contains the actual class.
        // Check if input data contains actual class. If not don't output accuracy or actual class
        String[] tuple = testInstance.getV();
        String resultClass = m.evaluate(tuple, featureValueSizes);
        if(resultClass.equalsIgnoreCase(tuple[tuple.length-1])){
          correct += 1;
        }
        total += 1;
        outToWriter.emit("Predicted: " + resultClass + "\tActual: " + tuple[tuple.length-1] + "\n");
      }
    }
  };

  @Override
  public void setup(OperatorContext context){

  }

  public void initModels(){
    if(nbc.isKFoldPartition()){ // Do this in input port from aggregator. Cache the models sent by the aggregator.
      //      for(int i=0;i<folds;i++){
      //        kFoldModels[i] = NBPMMLUtils.getModelFromPMMLFile(nbc.getOutputModelDir()+Path.SEPARATOR+nbc.getOutputModelFileName()+"."+i, nbc);
      //        kFeatureValueSizes[i] = kFoldModels[i].getFeatureValueSize();
      //      }
    }
    else{
      m = NBPMMLUtils.getModelFromPMMLFile(nbc.getOutputModelDir()+Path.SEPARATOR+nbc.getOutputModelFileName(), nbc);
      featureValueSizes = m.getFeatureValueSize();
    }
    LOG.info("Models Initialized");
  }

  @Override
  public void beginWindow(long windowId) {
  }

  @Override
  public void endWindow() {
    if(changeInWindow){
      changeInWindow = false;

      if(nbc.isKFoldPartition()){
        StringBuilder outputStats = new StringBuilder();
        for(int i=0;i<folds;i++){
          if(kFoldTotal[i] > 0){
            outputStats.append("Fold-" + i +": "+ (double)(kFoldCorrect[i]) / (double)(kFoldTotal[i]) + "\t");
          }
        }
        outputStats.append("\n");
        outToWriter.emit(outputStats.toString());
        //LOG.info("Emitted at end window: {}", outputStats.toString());
      }
    }
  }
}
