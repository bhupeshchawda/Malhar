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
 * This class will read the ARFF format files and pass on 
 * the corresponding strings after keeping the meta data in separate fields
 * @author 
 *
 */
public class ARFFReader extends BaseOperator{

	private static final Logger LOG = LoggerFactory.getLogger(ARFFReader.class);

	String relationName;
	public static String attrClass = "CLASS";
	HashMap<String, String> attributes;

	/**
	 * Output port that emits value of the fields.
	 * Output data type can be configured in the implementation of this operator.
	 */
	public final transient DefaultOutputPort<HashMap<String, String>> output = new DefaultOutputPort<HashMap<String, String>>();

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
				attributes = parseAsCsv(tuple);
				output.emit(attributes);
			}
		}
	};

	/**
	 * Parses the String as a csv tuple
	 * @param tuple
	 * @return Map of Attribute Name -> Attribute Value
	 */
	public static HashMap<String, String> parseAsCsv(String tuple){
		HashMap<String, String> retVal = new HashMap<String, String>();
		String[] attrs = tuple.trim().split(",");
		for(int i=0;i<attrs.length-1;i++){
			retVal.put(i+"",attrs[i]);
		}
		retVal.put(attrClass, attrs[attrs.length-1]);
		return retVal;
	}
}
