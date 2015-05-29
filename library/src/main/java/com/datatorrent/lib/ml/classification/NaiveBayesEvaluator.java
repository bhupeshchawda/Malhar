package com.datatorrent.lib.ml.classification;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

/**
 * A placeholder class which will read the ARFF format files and pass on 
 * the corresponding strings after keeping the meta data in separate fields
 * @author 
 *
 */
public class NaiveBayesEvaluator extends BaseOperator{

	private static final Logger LOG = LoggerFactory.getLogger(NaiveBayesEvaluator.class);

	transient HashMap<String, String> attributes = null;
	String pmmlFilePath;
	transient NaiveBayesModelStorage m;
	double correct = 0.0;
	double total = 0.0;
	
	double windowCorrect = 0.0;
	double windowTotal = 0.0;

	public void setPmmlFilePath(String path){
		this.pmmlFilePath = path;
	}

	/**
	 * Output port that emits value of the fields.
	 * Output data type can be configured in the implementation of this operator.
	 */
	public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
	public final transient DefaultOutputPort<String> stats = new DefaultOutputPort<String>();

	public final transient DefaultInputPort<HashMap<String, String>> input = new DefaultInputPort<HashMap<String, String>>(){
		
		@Override
		public void process(HashMap<String, String> tuple) {
			attributes = tuple;
			String resultClass = m.evaluate(attributes);
			if(resultClass.equalsIgnoreCase(tuple.get(ARFFReader.attrClass))){
				correct += 1;
				windowCorrect += 1;
			}
			total += 1;
			windowTotal += 1;
			output.emit("Result Class:" + resultClass + "\tAccuracy: " + correct/total + "\n");
			
		}
	};

	@Override
	public void setup(OperatorContext context){
		m = NaiveBayesPMMLUtils.getModelFromPMMLFile(pmmlFilePath);
	}
	
	@Override
	public void beginWindow(long windowId) {
		windowCorrect = 0.0;
		windowTotal = 0.0;
	}

	@Override
	public void endWindow() {
		if(stats.isConnected()){
			stats.emit("Accuracy (Window): " + windowCorrect/windowTotal + "\n");
		}
	}
}
