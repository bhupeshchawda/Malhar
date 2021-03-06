package com.datatorrent.lib.ml.classification;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.http.impl.client.NullBackoffStrategy;
import org.dmg.pmml.*;
import org.jpmml.model.JAXBUtil;

/**
 * This class acts as the storage for the Naive Bayes Classification algorithm.
 * Currently only supports categorical values for features. Can be extended for continuous features.
 * 
 * TODO Changes for continuous features: 
 * In PMML 4.2 onwards, there is a provision to store a distribution instead of just counts for each feature value appearing in the input instances.
 * PMML allows 4 types of distributions to be stored. Gaussian, Poission, Uniform or AnyDistribution. 
 * Since Weka assumes a Gaussian distribution, it may be fair to just assume a Gaussian distribution for the values of a continuous feature.
 * For continuous features, the probability may now be computed directly from the distribution by giving the feature value of the test instance.
 * 
 * @author bhupesh
 *
 */
@SuppressWarnings("serial")
public class NBModelStorage extends ClassificationModelStorage{

  /**
   * A map of String -> Integer keeping track of number of instances of each class that are received/processed
   */
  long[] classCounts;
  int numClasses;
  int numAttributes;

  /**
   * The feature table keeping the counts for <featureName, featureValue, ClassName> triplet
   * TODO: This can be optimized by using primitive types like arrays. 
   */
  HashMap<String, Long>[][] featureTableCategorical;

  /**
   * Constructor
   */
  public NBModelStorage(){

  }

  public NBModelStorage(int numAttributes, int numClasses){
    instanceCount = 0;
    this.numAttributes = numAttributes;
    this.numClasses = numClasses;
    classCounts = new long[numClasses];
    featureTableCategorical = new HashMap[numAttributes][numClasses];
  }

  /**
   * Clears and resets all the components of this object.
   */
  public void clear(){
    instanceCount = 0;

    // Clear class counts
    for(int i=0;i<numClasses;i++){
      classCounts[i] = 0;
    }

    // Clear feature table
    for(int i=0;i<numAttributes;i++){
      for(int j=0;j<numClasses;j++){
        featureTableCategorical[i][j] = null;
      }
    }
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

    int trainClass = Integer.parseInt(features[features.length-1]);
    classCounts[trainClass] += 1;

    for(int i=0;i<features.length-1;i++){
      int featureName = i;
      String featureValue = features[i];

      if(featureTableCategorical[featureName][trainClass] == null){
        HashMap<String, Long> featureValueTable = new HashMap<String, Long>();
        featureValueTable.put(featureValue, 1L);
        featureTableCategorical[featureName][trainClass] = featureValueTable;
      }
      else{
        HashMap<String, Long> featureValueTable  = featureTableCategorical[featureName][trainClass];
        if(featureValueTable.containsKey(featureValue)){
          featureValueTable.put(featureValue, featureValueTable.get(featureValue)+1);
        }
        else{
          featureValueTable.put(featureValue, 1L);
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
    for(int i=0;i<numAttributes;i++){      
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
    for(int i=0;i<numAttributes;i++){
      BayesInput bayesInput = new BayesInput(fieldNames.get(i));
      PairCounts pairCounts = null;
      ArrayList<PairCounts> pairCountList = new ArrayList<PairCounts>();
      for(String featureValue: getFeatureValues(i)){
        TargetValueCounts targetValueCounts = new TargetValueCounts();
        ArrayList<TargetValueCount> targetValueCountList = new ArrayList<TargetValueCount>();
        for(int j = 0; j < numClasses; j++){
          if(featureTableCategorical[i][j] != null && featureTableCategorical[i][j].containsKey(featureValue)){
            TargetValueCount targetValueCount = new TargetValueCount(j+"", featureTableCategorical[i][j].get(featureValue));
            targetValueCountList.add(targetValueCount);
          }
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
    for(int i=0;i<numClasses;i++){
      TargetValueCount targetValueCount = new TargetValueCount(i+"", classCounts[i]);
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
  @SuppressWarnings("unchecked")
  public void merge(ClassificationModelStorage ctm){

    NBModelStorage m = (NBModelStorage) ctm;
    if(m.instanceCount == 0)  return;

    instanceCount += m.instanceCount;

    for(int i=0;i<numClasses;i++){
      this.classCounts[i] += m.classCounts[i];
    }

    for(int i=0;i<numAttributes;i++){
      for(int j=0;j<numClasses;j++){
        HashMap<String, Long> hmThis = this.featureTableCategorical[i][j];
        HashMap<String, Long> hm = m.featureTableCategorical[i][j];
        if(hm == null){
          continue;
        }
        for(String fv: hm.keySet()){
          if(hmThis != null){
            if(hmThis.containsKey(fv)){
              hmThis.put(fv, hmThis.get(fv) + hm.get(fv));
            }
            else{
              hmThis.put(fv, hm.get(fv));
            }
          }
          else{
            hmThis = new HashMap<String, Long>();
            hmThis.putAll(hm);
            this.featureTableCategorical[i][j] = hmThis;
          }
        }
      }
    }
  }

  public int[] getFeatureValueSize(){
    //Store for each attribute what is the tutal number of feature values it has - irrespective of class
    int[] numFeatureValues = new int[numAttributes];
    HashSet<String> featureValues = new HashSet<String>();
    for(int i=0;i<numAttributes;i++){
      featureValues.clear();
      for(int j=0;j<numClasses;j++){
        if(featureTableCategorical[i][j] != null){
          featureValues.addAll(featureTableCategorical[i][j].keySet());
        }
      }
      numFeatureValues[i] = featureValues.size();
    }
    return numFeatureValues;
  }

  public String[] getFeatureValues(int featureName){
    HashSet<String> featureValues = new HashSet<String>();

    for(int j=0;j<numClasses;j++){
      if(featureTableCategorical[featureName][j] != null){
        featureValues.addAll(featureTableCategorical[featureName][j].keySet());
      }
    }

    return featureValues.toArray(new String[featureValues.size()]);
  }

  /**
   * Evaluates the input testInstance using the current model
   * 
   * @return String - The class label of the testInstance as predicted using the model
   */
  public String evaluate(String[] testInstance, int[] featureValueSizes){
    String predictedClass = "";
    double[] probabilities = new double[numClasses];

    double total = 0;
    
    for(int i=0;i<numClasses;i++){
      double denom = classCounts[i];
      double classScore = Math.log(denom) - Math.log(instanceCount);
      for(int j=0;j<numAttributes;j++){
        int numValues = featureValueSizes[j];
        String featureValue = testInstance[j];
        double numerator = 0;
        if(featureTableCategorical[j][i] != null && featureTableCategorical[j][i].containsKey(featureValue)){
          numerator = featureTableCategorical[j][i].get(featureValue);
        }
        else{
          numerator = 0;
        }
        classScore += Math.log(numerator+1) - Math.log(denom + numValues);
      }
      probabilities[i] = classScore;
      total += Math.exp(classScore);
    }

    double max = 0;
    for(int i=0;i<numClasses;i++){
      double classScore = probabilities[i];
      classScore = Math.exp(classScore - Math.log(total));
      probabilities[i] = classScore;
      if(max < classScore){
        max = classScore;
        predictedClass = i+"";
      }      
    }

    return predictedClass;
  }

  @Override
  public String evaluate(String[] testInstance)
  {
    // TODO Auto-generated method stub
    return null;
  }

}
