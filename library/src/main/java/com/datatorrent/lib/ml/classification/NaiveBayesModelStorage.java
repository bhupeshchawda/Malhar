package com.datatorrent.lib.ml.classification;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import org.dmg.pmml.*;
import org.jpmml.model.JAXBUtil;

/**
 * This class acts as the storage for the Naive Bayes Classification algorithm.
 * Currently only supports categorical values for features. Can be extended for continuous features.
 * 
 * Changes for continuous features: 
 * In PMML 4.2 onwards, there is a provision to store a distribution instead of just counts for each feature value appearing in the input instances.
 * PMML allows 4 types of distributions to be stored. Gaussian, Poission, Uniform or AnyDistribution. 
 * Since Weka assumes a Gaussian distribution, it may be fair to just assume a Gaussian distribution for the values of a continuous feature.
 * For continuous features, the probability may now be computed directly from the distribution by giving the feature value of the test instance.
 * 
 * @author bhupesh
 *
 */
@SuppressWarnings("serial")
public class NaiveBayesModelStorage extends ClassificationModelStorage{

	/**
	 * A map of String -> Integer keeping track of number of instances of each class that are received/processed
	 */
	HashMap<String, Integer> classCounts;

	/**
	 * The feature table keeping the counts for <featureName, featureValue, ClassName> triplet
	 * TODO: This can be optimized by using primitive types like arrays. 
	 */
	HashMap<String, HashMap<String, HashMap<String, Integer>>> featureTableCategorical;

	/**
	 * Constructor
	 */
	public NaiveBayesModelStorage(){
		instanceCount = 0;
		classCounts = new HashMap<String, Integer>();
		featureTableCategorical = new HashMap<String, HashMap<String,HashMap<String,Integer>>>();
	}

	/**
	 * Clears and resets all the components of this object.
	 */
	public void clear(){
		instanceCount = 0;
		classCounts.clear();
		featureTableCategorical.clear();
	}

	/**
	 * Processes the input training sample.
	 * Increments the instanceCount
	 * Adds the class in this training sample to the classCounts map
	 * Increments count for <featureName, featureValue, ClassName> triplet
	 */
	public void updateModel(HashMap<String, String> features){
		if(features == null || features.keySet().size() <= 1){
			return;
		}
		instanceCount ++;

		String trainClass = features.get(ARFFReader.attrClass);
		if(classCounts.containsKey(trainClass)){
			classCounts.put(trainClass, classCounts.get(trainClass)+1);
		}
		else{
			classCounts.put(trainClass, 1);
		}

		for(String attributeName: features.keySet()){
			if(attributeName.equalsIgnoreCase(ARFFReader.attrClass))	continue;
			//			if(continuousAttributes.contains(attributeName))	continue; // Don't do the rest for Continuous attributes
			String featureName = attributeName;
			String featureValue = features.get(featureName).trim();

			if(!featureTableCategorical.containsKey(featureName)){
				HashMap<String, HashMap<String, Integer>> featureValues = new HashMap<String, HashMap<String, Integer>>();
				HashMap<String, Integer> classCounts = new HashMap<String, Integer>();
				classCounts.put(trainClass, 1);
				featureValues.put(featureValue, classCounts);
				featureTableCategorical.put(featureName, featureValues);
			}
			else{
				if(!featureTableCategorical.get(featureName).containsKey(featureValue)){
					HashMap<String, Integer> classCounts = new HashMap<String, Integer>();
					classCounts.put(trainClass, 1);
					featureTableCategorical.get(featureName).put(featureValue, classCounts);
				}
				else{
					if(featureTableCategorical.get(featureName).get(featureValue).containsKey(trainClass)){
						featureTableCategorical.get(featureName).get(featureValue).put(trainClass, featureTableCategorical.get(featureName).get(featureValue).get(trainClass)+1);							
					}
					else{
						featureTableCategorical.get(featureName).get(featureValue).put(trainClass, 1);
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
		if(this.instanceCount == 0){
			return "";
		}
		Header header = new Header()
		.setCopyright("DataTorrent")
		.setDescription("Naive Bayes PMML");

		DataDictionary dataDictionary  = new DataDictionary();
		MiningSchema miningSchema = new MiningSchema();

		ArrayList<FieldName> fieldNames = new ArrayList<FieldName>();
		for(int i=0;i<this.featureTableCategorical.size();i++){			
			FieldName fieldName = FieldName.create(i+"");
			fieldNames.add(fieldName);
			// TODO: Implement reading header of ARFF file. 
			// This will allow us to identify the exact data types and op types for the data fields. 
			dataDictionary.addDataFields(new DataField(fieldName, 
					OpType.CATEGORICAL, 
					DataType.INTEGER));
			miningSchema.addMiningFields(new MiningField(fieldName));
		}
		dataDictionary.setNumberOfFields(dataDictionary.getDataFields().size());

		FieldName fieldNameClass = FieldName.create("Class");
		fieldNames.add(fieldNameClass);
		miningSchema.addMiningFields(new MiningField(fieldNameClass).setUsageType(FieldUsageType.TARGET));

		BayesInputs bayesInputs = new BayesInputs();
		ArrayList<BayesInput> bayesInputList = new ArrayList<BayesInput>();

		//For Categorical Attributes
		for(String featureName: featureTableCategorical.keySet()){
			BayesInput bayesInput = new BayesInput(fieldNames.get(Integer.parseInt(featureName)));
			HashMap<String, HashMap<String, Integer>> featureValueTable = featureTableCategorical.get(featureName);
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
	public void merge(ClassificationModelStorage ctm){

		NaiveBayesModelStorage m = (NaiveBayesModelStorage) ctm;
		if(m.instanceCount == 0)	return;

		instanceCount += m.instanceCount;

		for(String className: m.classCounts.keySet()){
			if(this.classCounts.containsKey(className)){
				this.classCounts.put(className, this.classCounts.get(className)+m.classCounts.get(className));
			}
			else{
				this.classCounts.put(className, m.classCounts.get(className));
			}
		}

		// Feature Table Categorical
		HashMap<String, HashMap<String,HashMap<String,Integer>>> f1 = m.featureTableCategorical;
		HashMap<String, HashMap<String,HashMap<String,Integer>>> f2 = this.featureTableCategorical;

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

	/**
	 * Evaluates the input testInstance using the current model
	 * 
	 * @return String - The class label of the testInstance as predicted using the model
	 */
	public String evaluate(HashMap<String, String> testInstance){
		String predictedClass = "";
		HashMap<String, Double> probabilities = new HashMap<String, Double>();

		double total = 0;
		for(String className: classCounts.keySet()){
			double denom = classCounts.get(className);
			double classScore = Math.log(denom) - Math.log(instanceCount);

			for(String featureName: featureTableCategorical.keySet()){
				int numValues = featureTableCategorical.get(featureName).keySet().size();
				String featureValue = testInstance.get(featureName);
				double numerator = 0;
				if(featureTableCategorical.get(featureName).containsKey(featureValue) &&
						featureTableCategorical.get(featureName).get(featureValue).containsKey(className)){
					numerator = featureTableCategorical.get(featureName).get(featureValue).get(className);
				}
				else{
					numerator = 0;
				}
				classScore += Math.log(numerator+1) - Math.log(denom + numValues);
			}

			probabilities.put(className, classScore);
			total += Math.exp(classScore);
		}

		double max = 0;
		for(String className: classCounts.keySet()){
			double classScore = probabilities.get(className);
			classScore = Math.exp(classScore - Math.log(total));
			probabilities.put(className, classScore);
			if(max < classScore){
				max = classScore;
				predictedClass = className;
			}
		}

		return predictedClass;
	}

}
