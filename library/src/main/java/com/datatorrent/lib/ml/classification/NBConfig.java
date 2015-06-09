package com.datatorrent.lib.ml.classification;

/**
 * This class holds the configuration for the Naive Bayes Classifier. 
 * This configuration has attributes defined for Training as well as Testing the model.
 * This also has attributes needed for the k- fold cross validation flow, which is used to determine the accuracy of the model
 * 
 * @author bhupesh
 *
 */
public class NBConfig {

  private boolean isKFoldPartition = false;
  private boolean onlyTrain = false;
  private boolean onlyEvaluate = false;
  private int numFolds = 1;
  private int numAttributes;
  private int numClasses;
  private String inputDataFile;
  private String outputModelDir;
  private String outputModelFileName;
  private String outputResultDir;
  private String outputResultFileName;
  
  public NBConfig(){
    
  }
  
  /**
   * Constructor for NBConfig. Not all attributes are needed in all cases.
   * 
   * @param isKFold - Is this a k fold Training or Evaluation flow?
   * @param numFolds - Number of folds. k = ?
   * @param inputDataDir - Input Data for Training the model
   * @param outputModelDir - Output directory for storing the model
   * @param outputModelFileName - File name of the model. In case of k fold cross validation flow, the base name of the model. The different models will have the model number appended to it.
   * @param outputResultDir - Output directory to store the results of the Evaluation or the K-fold Evaluation
   * @param outputResultFileName - Output filename for the results of the Evaluation or the K-fold Evaluation
   * @param overwriteOutput - Whether to overwrite the output file? In case of Training, it is the model file - true. In case of Evaluation, it is the result file - false
   */
  public NBConfig(boolean isKFold, boolean onlyTrain, boolean onlyEvaluate, int numFolds, int numAttributes, 
      int numClasses, String inputDataDir, String outputModelDir, String outputModelFileName, 
      String outputResultDir, String outputResultFileName){  
    this.isKFoldPartition = isKFold;
    this.onlyTrain = onlyTrain;
    this.onlyEvaluate = onlyEvaluate;
    this.numFolds = numFolds;
    this.numAttributes = numAttributes;
    this.numClasses = numClasses;
    this.inputDataFile = inputDataDir;
    this.outputModelDir = outputModelDir;
    this.outputModelFileName = outputModelFileName;
    this.outputResultDir = outputResultDir;
    this.outputResultFileName = outputResultFileName;
  }

  /*
   * Getters and Setters
   */
  
  public boolean isKFoldPartition() {
    return isKFoldPartition;
  }

  public void setKFoldPartition(boolean isKFoldPartition) {
    this.isKFoldPartition = isKFoldPartition;
  }

  public int getNumFolds() {
    return numFolds;
  }

  public void setNumFolds(int numFolds) {
    this.numFolds = numFolds;
  }

  public String getInputDataFile() {
    return inputDataFile;
  }

  public void setInputDataFile(String inputDataFile) {
    this.inputDataFile = inputDataFile;
  }

  public String getOutputModelDir() {
    return outputModelDir;
  }

  public void setOutputModelDir(String outputModelDir) {
    this.outputModelDir = outputModelDir;
  }

  public String getOutputModelFileName() {
    return outputModelFileName;
  }

  public void setOutputModelFileName(String outputModelFileName) {
    this.outputModelFileName = outputModelFileName;
  }

  public String getOutputResultDir() {
    return outputResultDir;
  }

  public void setOutputResultDir(String outputResultDir) {
    this.outputResultDir = outputResultDir;
  }

  public String getOutputResultFileName() {
    return outputResultFileName;
  }

  public void setOutputResultFileName(String outputResultFileName) {
    this.outputResultFileName = outputResultFileName;
  }

  public boolean isOnlyTrain() {
    return onlyTrain;
  }

  public void setOnlyTrain(boolean onlyTrain) {
    this.onlyTrain = onlyTrain;
  }

  public boolean isOnlyEvaluate() {
    return onlyEvaluate;
  }

  public void setOnlyEvaluate(boolean onlyEvaluate) {
    this.onlyEvaluate = onlyEvaluate;
  }
  
  public int getNumAttributes()
  {
    return numAttributes;
  }

  public void setNumAttributes(int numAttributes)
  {
    this.numAttributes = numAttributes;
  }

  public int getNumClasses()
  {
    return numClasses;
  }

  public void setNumClasses(int numClasses)
  {
    this.numClasses = numClasses;
  }
}
