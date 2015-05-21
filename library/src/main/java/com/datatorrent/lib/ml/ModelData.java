package com.datatorrent.lib.ml;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.dmg.pmml.*;
import org.jpmml.model.JAXBUtil;

//TODO Create a super class which will act as the superclass of this class

/**
 * This class holds the model for Naive Bayes Classification algorithm.
 * 
 * @author bhupesh
 *
 */
public class ModelData implements Serializable{

	/**
	 * Serial version UID
	 */
	private static final long serialVersionUID = -6582920867670603439L;

	/**
	 * Number of instances of training samples received/processed.
	 */
	int instanceCount;
	
	/**
	 * A map of String -> Integer keeping track of number of instances of each class that are received/processed
	 */
	HashMap<String, Integer> classCounts;
	
	/**
	 * The feature table keeping the counts for <featureName, featureValue, ClassName> triplet
	 */
	HashMap<String, HashMap<String, HashMap<String, Integer>>> featureTable;

	/**
	 * Constructor for ModelData
	 */
	public ModelData(){
		instanceCount = 0;
		classCounts = new HashMap<String, Integer>();
		featureTable = new HashMap<String, HashMap<String,HashMap<String,Integer>>>();
	}

	/**
	 * Clears and resets all the components of this object.
	 */
	public void clear(){
		instanceCount = 0;
		classCounts.clear();
		featureTable.clear();
	}
	
	/**
	 * Processes the input training sample.
	 * Increments the instanceCount
	 * Adds the class in this training sample to the classCounts map
	 * Increments count for <featureName, featureValue, ClassName> triplet
	 */
	public void updateModel(String[] features){
		if(features == null || features.length <= 1){
			return;
		}
		instanceCount ++;

		String trainClass = features[features.length-1];
		if(classCounts.containsKey(trainClass)){
			classCounts.put(trainClass, classCounts.get(trainClass)+1);
		}
		else{
			classCounts.put(trainClass, 1);
		}

		for(int i=0;i<features.length-1;i++){
			String featureName = i+"";
			String featureValue = features[i].trim();

			if(!featureTable.containsKey(featureName)){
				HashMap<String, HashMap<String, Integer>> featureValues = new HashMap<String, HashMap<String, Integer>>();
				HashMap<String, Integer> classCounts = new HashMap<String, Integer>();
				classCounts.put(trainClass, 1);
				featureValues.put(featureValue, classCounts);
				featureTable.put(featureName, featureValues);
			}
			else{
				if(!featureTable.get(featureName).containsKey(featureValue)){
					HashMap<String, Integer> classCounts = new HashMap<String, Integer>();
					classCounts.put(trainClass, 1);
					featureTable.get(featureName).put(featureValue, classCounts);
				}
				else{
					if(featureTable.get(featureName).get(featureValue).containsKey(trainClass)){
						featureTable.get(featureName).get(featureValue).put(trainClass, featureTable.get(featureName).get(featureValue).get(trainClass)+1);							
					}
					else{
						featureTable.get(featureName).get(featureValue).put(trainClass, 1);
					}
				}
			}
		}
	}


	/**
	 * Exports the current object in a PMML format. The string is formatted in XML.
	 * jpmml-model library is used to convert internal data structures to a PMML XML model
	 * @return PMML format in form of XML string
	 */
	
	public String exportToPMML(){
		Header header = new Header()
		.setCopyright("DataTorrent")
		.setDescription("Naive Bayes PMML");

		DataDictionary dataDictionary  = new DataDictionary();
		MiningSchema miningSchema = new MiningSchema();
		
		ArrayList<FieldName> fieldNames = new ArrayList<FieldName>();
		for(int i=0;i<this.featureTable.size();i++){			
			FieldName fieldName = FieldName.create(i+"");
			fieldNames.add(fieldName);
			dataDictionary.addDataFields(new DataField(fieldName, 
					OpType.CATEGORICAL, 
					DataType.DOUBLE));
			miningSchema.addMiningFields(new MiningField(fieldName));
		}
		dataDictionary.setNumberOfFields(dataDictionary.getDataFields().size());

		FieldName fieldNameClass = FieldName.create("Class");
		fieldNames.add(fieldNameClass);
		miningSchema.addMiningFields(new MiningField(fieldNameClass).setUsageType(FieldUsageType.TARGET));
		
		BayesInputs bayesInputs = new BayesInputs();
		ArrayList<BayesInput> bayesInputList = new ArrayList<BayesInput>();

		for(String featureNum: featureTable.keySet()){
			BayesInput bayesInput = new BayesInput(fieldNames.get(Integer.parseInt(featureNum)));
			HashMap<String, HashMap<String, Integer>> featureValueTable = featureTable.get(featureNum);
			PairCounts pairCounts = null;
			ArrayList<PairCounts> pairCountList = new ArrayList<PairCounts>();
			for(String featureValue: featureValueTable.keySet()){
				HashMap<String, Integer> classTable = featureValueTable.get(featureValue);

				TargetValueCounts targetValueCounts = new TargetValueCounts();
				ArrayList<TargetValueCount> targetValueCountList = new ArrayList<TargetValueCount>();
				for(String className: classTable.keySet()){
					TargetValueCount targetValueCount = new TargetValueCount(className, classTable.get(className));
					targetValueCountList.add(targetValueCount);
				}
				targetValueCounts.addTargetValueCounts(targetValueCountList.toArray(new TargetValueCount[targetValueCountList.size()]));
				pairCounts = new PairCounts(featureValue, targetValueCounts);
				pairCountList.add(pairCounts);
			}
			bayesInput.addPairCounts(pairCountList.toArray(new PairCounts[pairCountList.size()]));
			bayesInputList.add(bayesInput);
		}
		bayesInputs.addBayesInputs(bayesInputList.toArray(new BayesInput[bayesInputList.size()]));

		BayesOutput bayesOutput = new BayesOutput();
		TargetValueCounts targetValueCounts = new TargetValueCounts();
		for(String className: classCounts.keySet()){
			TargetValueCount targetValueCount = new TargetValueCount(className, classCounts.get(className));
			targetValueCounts.addTargetValueCounts(targetValueCount);
		}
		bayesOutput.setFieldName(fieldNameClass);
		bayesOutput.setTargetValueCounts(targetValueCounts);

		NaiveBayesModel n = new NaiveBayesModel(1/instanceCount, MiningFunctionType.CLASSIFICATION, miningSchema, bayesInputs, bayesOutput);

		PMML pmml = new PMML("4.2", header, dataDictionary);
		pmml.addModels(n);

		OutputStream outputStream = null;
		try {

			Marshaller marshaller = JAXBUtil.createMarshaller();

			outputStream = new OutputStream() {	
				private StringBuilder string = new StringBuilder();
				@Override
				public void write(int b) throws IOException {
					this.string.append((char) b );
				}

				public String toString(){
					return this.string.toString();
				}
			};

			marshaller.marshal(pmml, outputStream);
		} catch (JAXBException e) {
			throw new RuntimeException();
		}

		return outputStream.toString();
	}

	
	/**
	 * Merges the input parameter ModelData m into the current object.
	 * After this call, the current object will have all the information from the input parameter.
	 * @param m
	 */
	public void merge(ModelData m){

		instanceCount += m.instanceCount;

		for(String className: m.classCounts.keySet()){
			if(this.classCounts.containsKey(className)){
				this.classCounts.put(className, this.classCounts.get(className)+m.classCounts.get(className));
			}
			else{
				this.classCounts.put(className, m.classCounts.get(className));
			}
		}

		// Feature Table
		HashMap<String, HashMap<String,HashMap<String,Integer>>> f1 = m.featureTable;
		HashMap<String, HashMap<String,HashMap<String,Integer>>> f2 = this.featureTable;

		for(String fn: f1.keySet()){
			if(f2.containsKey(fn)){
				HashMap<String, HashMap<String, Integer>> vfn2 = f2.get(fn);
				HashMap<String, HashMap<String, Integer>> vfn1 = f1.get(fn);
				for(String fv: f1.get(fn).keySet()){
					if(vfn2.containsKey(fv)){
						HashMap<String, Integer> vfv2 = vfn2.get(fv);
						HashMap<String, Integer> vfv1 = vfn1.get(fv);
						for(String c: f1.get(fn).get(fv).keySet()){
							if(vfv2.containsKey(c)){
								vfv2.put(c, vfv2.get(c)+vfv1.get(c));
							}
							else{
								vfv2.put(c, vfv1.get(c));
							}
						}
					}
					else{
						vfn2.put(fv, vfn1.get(fv));
					}
				}
			}
			else{
				f2.put(fn, f1.get(fn));
			}
		}

	}

}
