package com.datatorrent.lib.ml.classification;

import java.util.ArrayList;
import java.util.HashMap;
import javax.xml.bind.Unmarshaller;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dmg.pmml.BayesInput;
import org.dmg.pmml.BayesInputs;
import org.dmg.pmml.NaiveBayesModel;
import org.dmg.pmml.PMML;
import org.dmg.pmml.PairCounts;
import org.dmg.pmml.TargetValueCount;
import org.dmg.pmml.TargetValueCounts;
import org.jpmml.model.JAXBUtil;

/**
 * This class provides utility functions for Naive Bayes classification.
 * 
 * @author bhupesh
 *
 */

public class NaiveBayesPMMLUtils {
	
	/**
	 * Extracts the NaiveBayesModelStorage object from the PMML xml file stored at the parameter pmmlFilePath
	 * 
	 * @param pmmlFilePath
	 * @return an instance of NaiveBayesModelStorage - hosls a Naive Bayes model represented by the Pmml file
	 */
	public static NaiveBayesModelStorage getModelFromPMMLFile(String pmmlFilePath){
		try{
			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path(pmmlFilePath));
//			BufferedReader in = new BufferedReader(new FileReader(pmmlFilePath));
			Unmarshaller unmarshaller = JAXBUtil.createUnmarshaller();
			PMML pmml = (PMML) unmarshaller.unmarshal(in);
			NaiveBayesModelStorage m = getModelFromPMML(pmml);
			return m;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException();
		}
	}
	
	/**
	 * Utility function for extracting the Naive Bayes model from PMML model
	 * 
	 * @param pmml
	 * @return an instance of NaiveBayesModelStorage
	 */
	public static NaiveBayesModelStorage getModelFromPMML(PMML pmml){
		NaiveBayesModelStorage m = new NaiveBayesModelStorage();
		NaiveBayesModel model = (NaiveBayesModel) pmml.getModels().get(0);
		BayesInputs bayesInputs = model.getBayesInputs();
		for(BayesInput bayesInput: bayesInputs){
			String fieldName = bayesInput.getFieldName().getValue();
			ArrayList<PairCounts> pcs = (ArrayList<PairCounts>) bayesInput.getPairCounts();
			HashMap<String, HashMap<String, Integer>> featureValueMap = null;
			if(m.featureTableCategorical.containsKey(fieldName)){
				featureValueMap = m.featureTableCategorical.get(fieldName);
			}
			else{
				featureValueMap = new HashMap<String, HashMap<String,Integer>>();
			}

			for(PairCounts pc: pcs){
				String fieldValue = pc.getValue();
				TargetValueCounts tvcs = pc.getTargetValueCounts();

				HashMap<String, Integer> classCountMap = null;
				if(featureValueMap.containsKey(fieldValue)){
					classCountMap = featureValueMap.get(fieldValue);
				}
				else{
					classCountMap = new HashMap<String, Integer>();
				}

				for(TargetValueCount tvc: tvcs){
					String className = tvc.getValue();
					int count = (int) tvc.getCount();
					if(classCountMap.containsKey(className)){
						classCountMap.put(className, classCountMap.get(className)+count);
					}
					else{
						classCountMap.put(className, count);
					}
					if(m.classCounts.containsKey(className)){
						m.classCounts.put(className, m.classCounts.get(className)+count);
					}
					else{
						m.classCounts.put(className, count);
					}
					m.instanceCount += count;
				}
				featureValueMap.put(fieldValue, classCountMap);
			}			
			m.featureTableCategorical.put(fieldName, featureValueMap);
		}
		return m;
	}

}
