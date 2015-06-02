package com.datatorrent.lib.ml.classification;

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

	transient String[] attributes = null;
	String pmmlFilePath;
	transient NBModelStorage m;
	double correct = 0.0;
	double total = 0.0;

	//	double windowCorrect = 0.0;
	//	double windowTotal = 0.0;

	NBConfig nbc = null;

	transient NBModelStorage[] kFoldModels;
	int folds = 0;
	long[] kFoldCorrect;
	long[] kFoldTotal;

	public NBEvaluator(){
	}

	public NBEvaluator(NBConfig nbc){
		this.nbc = nbc;
	}

	public void setPmmlFilePath(String path){
		this.pmmlFilePath = path;
	}

	/**
	 * Output port that emits the predicted class along with the cumulative accuracy
	 */
	public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

	/**
	 * Output port that emits statistics per window. 
	 * TODO: Implement per window statistics.
	 */
	public final transient DefaultOutputPort<String> stats = new DefaultOutputPort<String>();

	/**
	 * Input port for evaluation of test data against a single Naive Bayes model. 
	 * This input port receives a String[]. 
	 * The input tuple contains the attributes with the class value as the last element in the array.
	 */
	public final transient DefaultInputPort<String[]> input = new DefaultInputPort<String[]>(){

		@Override
		public void process(String[] instance) {
			String resultClass = m.evaluate(instance);
			if(resultClass.equalsIgnoreCase(instance[instance.length-1])){
				correct += 1;
			}
			total += 1;
			output.emit("Result Class:" + resultClass + "\tAccuracy: " + correct/total + "\n");

		}
	};

	/**
	 * Input port for k-fold cross validation flow. This is for identifying the accuracy of the Naive Bayes model.
	 * This port receives an entry of the form <Integer, String[]>. 
	 * The key in the entry points to the "fold" of the input data. 
	 * In other words, it gives the model against which this input tuple (pointed to by the value) needs to be evaluated.
	 */
	public final transient DefaultInputPort<MapEntry<Integer,String[]>> kFoldInput = 
			new DefaultInputPort<MapEntry<Integer,String[]>>(){

		@Override
		public void process(MapEntry<Integer, String[]> testInstance) {
			if(nbc.isKFoldPartition()){
				int fold = testInstance.getK();
				String predictedClass = kFoldModels[fold].evaluate(testInstance.getV());
				String originalClass = testInstance.getV()[testInstance.getV().length-1];
				if(predictedClass.trim().equalsIgnoreCase(originalClass.trim())){
					kFoldCorrect[fold] += 1;
				}
				kFoldTotal[fold] += 1;
			}
		}
	};

	@Override
	public void setup(OperatorContext context){
		if(nbc.isKFoldPartition()){
			folds = nbc.getNumFolds();
			kFoldModels = new NBModelStorage[folds];
			for(int i=0;i<folds;i++){
				kFoldModels[i] = NBPMMLUtils.getModelFromPMMLFile(pmmlFilePath+"."+i);
			}
			kFoldCorrect = new long[folds];
			kFoldTotal = new long[folds];
		}
		else{
			m = NBPMMLUtils.getModelFromPMMLFile(pmmlFilePath);
		}
	}

	@Override
	public void beginWindow(long windowId) {
	}

	@Override
	public void endWindow() {

		if(nbc.isKFoldPartition()){
			StringBuilder outputStats = new StringBuilder();
			outputStats.append("Accuracies = ");
			for(int i=0;i<folds;i++){
				if(kFoldTotal[i] > 0){
					outputStats.append( i +": "+ (double)(kFoldCorrect[i]) / (double)(kFoldTotal[i]) + "\t");
				}
			}
			outputStats.append("\n");
			output.emit(outputStats.toString());
		}
	}
}
