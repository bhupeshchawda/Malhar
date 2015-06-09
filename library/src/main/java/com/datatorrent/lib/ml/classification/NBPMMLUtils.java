package com.datatorrent.lib.ml.classification;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.bind.Unmarshaller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dmg.pmml.BayesInput;
import org.dmg.pmml.BayesInputs;
import org.dmg.pmml.BayesOutput;
import org.dmg.pmml.NaiveBayesModel;
import org.dmg.pmml.PMML;
import org.dmg.pmml.PairCounts;
import org.dmg.pmml.TargetValueCount;
import org.dmg.pmml.TargetValueCounts;
import org.jpmml.model.JAXBUtil;

/**
 * This class provides utility functions for Naive Bayes classification.
 * @author bhupesh
 *
 */

public class NBPMMLUtils {
  
  /**
   * Extracts the NaiveBayesModelStorage object from the PMML xml file stored at the parameter pmmlFilePath
   * 
   * @param pmmlFilePath
   * @return an instance of NaiveBayesModelStorage - hosls a Naive Bayes model represented by the Pmml file
   */
  public static NBModelStorage getModelFromPMMLFile(String pmmlFilePath, NBConfig nbc){
    try{
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      FSDataInputStream in = fs.open(new Path(pmmlFilePath));
//      BufferedReader in = new BufferedReader(new FileReader(pmmlFilePath));
      Unmarshaller unmarshaller = JAXBUtil.createUnmarshaller();
      PMML pmml = (PMML) unmarshaller.unmarshal(in);
      NBModelStorage m = getModelFromPMML(pmml, nbc);
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
  public static NBModelStorage getModelFromPMML(PMML pmml, NBConfig nbc){
    NBModelStorage m = new NBModelStorage(nbc.getNumAttributes(), nbc.getNumClasses());
    NaiveBayesModel model = (NaiveBayesModel) pmml.getModels().get(0);
    BayesInputs bayesInputs = model.getBayesInputs();
    
    for(BayesInput bayesInput: bayesInputs){
      String fieldName = bayesInput.getFieldName().getValue();
      int i = Integer.parseInt(fieldName.trim());
      ArrayList<PairCounts> pcs = (ArrayList<PairCounts>) bayesInput.getPairCounts();
      
      for(PairCounts pc: pcs){
        String fieldValue = pc.getValue();
        TargetValueCounts tvcs = pc.getTargetValueCounts();

        for(TargetValueCount tvc: tvcs){
          String className = tvc.getValue();
          int j = Integer.parseInt(className);
          long count = (long) tvc.getCount();
          
          if(m.featureTableCategorical[i][j] == null){
            HashMap<String, Long> featureValueTable = new HashMap<String, Long>();
            featureValueTable.put(fieldValue, count);
            m.featureTableCategorical[i][j] = featureValueTable;
          }
          else{
            HashMap<String, Long> featureValueTable = m.featureTableCategorical[i][j];
            if(featureValueTable.containsKey(fieldValue)){
              featureValueTable.put(fieldValue, featureValueTable.get(fieldValue)+count);
            }
            else{
              featureValueTable.put(fieldValue, count);
            }
          }
          
        }
      }
    }
    
    BayesOutput bayesOutput = model.getBayesOutput();
    for(TargetValueCount tvc: bayesOutput.getTargetValueCounts()){
      String className = tvc.getValue();
      double count = tvc.getCount();
      m.classCounts[Integer.parseInt(className)] = (long) count;
      m.instanceCount += count;
    }
    
    return m;
  }

}
