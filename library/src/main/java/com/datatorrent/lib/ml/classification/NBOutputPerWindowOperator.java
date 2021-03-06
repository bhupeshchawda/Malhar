package com.datatorrent.lib.ml.classification;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.helpers.LogLog;
import org.eclipse.jetty.util.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;

/**
 * This is the Naive Bayes Output operator for overwriting output file on HDFS..
 * Custom created, since AbstractFileOutputOperator does not have the capability to overwrite files.
 *
 */
public class NBOutputPerWindowOperator extends BaseOperator {

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(NBOutputPerWindowOperator.class);

  String tuple = "";
  NBConfig nbc = null;
  transient FSDataOutputStream[] kFoldOuts = null;
  transient FSDataOutputStream trainOut = null;
  transient FSDataOutputStream evalOut = null;
  transient FileSystem fs = null;

  int folds;
  String[] xmlModels;
  boolean changeInWindow = false;
  StringBuilder dataToWrite;

  public NBOutputPerWindowOperator(){
  }

  public NBOutputPerWindowOperator(NBConfig nbc){
    this.nbc = nbc;
    dataToWrite = new StringBuilder();
  }

  // For Only Evaluation
  public final transient DefaultInputPort<String> inStringWriter = 
      new DefaultInputPort<String>() {
    @Override
    public void process(String tuple) {
      if(tuple != null && tuple.trim().length() != 0){
        dataToWrite.append(tuple);
        changeInWindow = true;
        //writeData(nbc.getOutputResultDir(), nbc.getOutputResultFileName(), false, tuple);
      }
    }
  };

  public final transient DefaultInputPort<Boolean> controlIn = new DefaultInputPort<Boolean>() {
    @Override
    public void process(Boolean b)
    {
      if(b.booleanValue()){
        LOG.debug("Training Done");
        writeModels();
        try{
          //Write done file
          Path doneFile = new Path(nbc.getOutputModelDir() + Path.SEPARATOR + ".done");
          Configuration conf = new Configuration();
          FileSystem fs = FileSystem.get(conf);
          fs.create(doneFile);
        }catch(IOException e){
          e.printStackTrace();
          throw new RuntimeException();
        }
      }
    }
  };
  
  // Training. K fold or plain training
  public final transient DefaultInputPort<MapEntry<Integer, String>> inMultiWriter = 
      new DefaultInputPort<MapEntry<Integer, String>>() {
    @Override
    public void process(MapEntry<Integer, String> xmlModel) {
//      if(xmlModel.getV() == null){
//        LOG.debug("Training Done");
//        writeModels();
//        try{
//          //Write done file
//          Path doneFile = new Path(nbc.getOutputModelDir() + Path.SEPARATOR + ".done");
//          Configuration conf = new Configuration();
//          FileSystem fs = FileSystem.get(conf);
//          fs.create(doneFile);
//        }catch(IOException e){
//          e.printStackTrace();
//          throw new RuntimeException();
//        }
//        return;
//      }
      if(nbc.isKFoldPartition() && !xmlModel.getK().equals(-1)){ // Only Store. Write at end window
        int fold = xmlModel.getK();
        xmlModels[fold] = xmlModel.getV();
        changeInWindow = true;
      }
      if(nbc.isOnlyTrain() && xmlModel.getK().equals(-1)){ // Only Store. Write at end window
        tuple = xmlModel.getV();
        changeInWindow = true;
      }
    }
  };

  @Override
  public void setup(OperatorContext context){
    try {
      fs = getFSInstance();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if(nbc.isKFoldPartition()){
      folds = nbc.getNumFolds();
      xmlModels = new String[folds];
      kFoldOuts = new FSDataOutputStream[folds];
    }
  }
  
  @Override
  public void teardown() {
    super.teardown();
    try {
      fs.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void endWindow(){
    if(changeInWindow){
      changeInWindow = false;
      if(dataToWrite != null && dataToWrite.toString().trim().length() > 0){
        writeData(nbc.getOutputResultDir(), nbc.getOutputResultFileName(), false, dataToWrite.toString());
        dataToWrite = new StringBuilder();
      }
    }
  }

  public void writeModels(){
    if(nbc.isKFoldPartition()){
      for(int i=0;i<xmlModels.length;i++){
        writeData(nbc.getOutputModelDir(), nbc.getOutputModelFileName()+"."+i, true, xmlModels[i]);
      }
    }
    if(nbc.isOnlyTrain()){      
      writeData(nbc.getOutputModelDir(), nbc.getOutputModelFileName(), true, tuple);
    }
  }
  
  public void writeData(String filePath, String fileName, boolean overwrite, String tuple){

    FSDataOutputStream out = null;
    try {
      Path writePath = new Path(filePath + Path.SEPARATOR + fileName);
      if(overwrite){
        out = fs.create(writePath, overwrite);
      }
      else{
        if(fs.exists(writePath)){
          out = fs.append(writePath);
        }
        else{
          out = fs.create(writePath, overwrite);
        }
      }

      out.writeBytes(tuple);
      out.hflush();
      out.close();

    } catch (IOException e) {
      e.printStackTrace();
    }  
  }

  public FileSystem getFSInstance() throws IOException
  {
    Configuration conf = new Configuration();
    return FileSystem.get(conf);
  }


}
