package com.datatorrent.lib.ml.classification;

import java.io.Serializable;
import java.util.HashMap;

import org.dmg.pmml.*;

/**
 * This class serves as a base class for implementation of Classification Predictive Algorithms.
 * With more child classes being added, this will mature with more data structures and methods.
 * 
 * @author bhupesh
 *
 */
public abstract class ClassificationModelStorage implements Serializable{

  /**
   * Serial version UID. Not defined in the child classes.
   */
  private static final long serialVersionUID = -9133012985548549551L;

  /**
   * Number of instances of training samples used to generate the model
   */
  int instanceCount;

  /**
   * Constructor
   */
  public ClassificationModelStorage(){
    instanceCount = 0;
  }

  /**
   * Clears and resets all the common components of the model
   */
  public void clear(){
    instanceCount = 0;
  }

  /**
   * Processes the input training sample.
   * Updates the model after looking at the input training instance.
   */
  public abstract void updateModel(String[] features);

  /**
   * Merges the input parameter ClassificationModelStorage s into the current object.
   * After this call, the current object will have all the information from the input parameter.
   * 
   * @param m
   */
  public abstract void merge(ClassificationModelStorage s);
  
  /**
   * Exports the current object in a PMML format. The string is formatted in XML.
   * jpmml-model library is used to convert internal data structures to a PMML XML model
   * 
   * @return PMML format in form of XML string
   */
  public abstract String exportToPMML();

  /**
   * This uses the current model to evaluate the class label for the input testInstance
   * 
   * @param testInstance
   * @return String - The class label which is predicted for the testInstance
   */
  public abstract String evaluate(String[] testInstance, int[] featureValueSizes);
  public abstract String evaluate(String[] testInstance);
  
}
