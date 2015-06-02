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

	String relationName;
	String[] attributes;
	NBConfig nbc = null;
	
	public NBInputReader(){
	}
	
	public NBInputReader(NBConfig nbc){
		this.nbc = nbc;
	}
	
	/**
	 * Output port that emits value of the fields.
	 * Output data type can be configured in the implementation of this operator.
	 */
	public final transient DefaultOutputPort<String[]> output = new DefaultOutputPort<String[]>();
	public final transient DefaultOutputPort<MapEntry<Integer,String[]>> kFoldOutput = new DefaultOutputPort<MapEntry<Integer,String[]>>();

	/**
	 * Input port receives tuples as String 
	 */
	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>(){

		@Override
		public void process(String tuple) {
			if(tuple.trim().startsWith("@") || tuple.trim().startsWith("%") || tuple.trim().length() == 0){
				LOG.info("Metadata String. Ignoring for now.");
			}
			else{
				if(nbc.isKFoldPartition()){
					if(kFoldOutput.isConnected()){
						int fold = Math.abs(tuple.hashCode())%nbc.getNumFolds();
						attributes = parseAsCsv(tuple);
						MapEntry<Integer, String[]> partitionTuple = new MapEntry<Integer, String[]>(fold, attributes);
						kFoldOutput.emit(partitionTuple);
					}
					else{
						LOG.info("No operator connected to foldValidationOutput port.");
					}
				}
				else{
					attributes = parseAsCsv(tuple);
					output.emit(attributes);
				}
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

}
