package com.datatorrent.lib.ml.classification;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This is the File line reader operator responsible for reading a file on HDFS
 * and sends a line as a tuple
 * 
 * @author
 *
 */
public class NBLineReader implements InputOperator {

  private static final Logger LOG = LoggerFactory
      .getLogger(NBLineReader.class);

  private long maxTuplesPerWindow;


  public final transient DefaultOutputPort<String> lineOutputPort = new DefaultOutputPort<String>();
  private transient BufferedReader br = null;
  private transient FileSystem fs = null;
  private NBConfig nbc = null;
  private boolean trainingPhase;
  private boolean evaluationPhase;
  private long counter = 0;

  public NBLineReader() {

  }

  public NBLineReader(NBConfig nbc) {
    this.nbc = nbc;
    if (nbc.isKFoldPartition()) {
      trainingPhase = true;
      evaluationPhase = false;
    }
  }

  protected String readEntity() {
    try {
      if (nbc.isKFoldPartition()) { // Both Training and Testing Phase is
        if (trainingPhase) {
          String s = br.readLine();
          if (s != null) {
            return s;
          } 
          else {
            resetReader();
            trainingPhase = false;
            LOG.debug("File System Reset. Training Phase Ended.");
            return "@FILE_END";
          }
        }
        else {
          String s = br.readLine();
          if (s != null) {
            return s;
          } else {
            return "";
          }
        }
      } else { // nbc.isOnlyTrain() || nbc.isOnlyEvaluate() - Only
        String s = br.readLine();
        if (s != null) {
          return s;
        } else {
          return "";
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void endWindow() {
    if (!trainingPhase) { // Begin evaluation in next window
      try {
        Path doneFile = new Path(nbc.getOutputModelDir() + Path.SEPARATOR + ".done");
        if (fs.exists(doneFile)) {
          evaluationPhase = true;
          fs.delete(doneFile, true);
          LOG.debug("Evaluation Phase Started");
        }
      } catch (IOException e) {
        throw new RuntimeException();
      }
    }
  }

  @Override
  public void beginWindow(long arg0) {
    counter = 0;
  }

  @Override
  public void setup(OperatorContext context) {
    initFileSystem();
    resetReader();
  }

  public void initFileSystem() {
    try{
      Configuration conf = new Configuration();
      fs = FileSystem.get(conf);
    }
    catch(IOException e){
      throw new RuntimeException(e);
    }
  }

  public void resetReader() {
    try{
      Path filePath = new Path(nbc.getInputDataFile());
      br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
    }
    catch(IOException e){
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown(){
    LOG.debug("Closing File System");
    br = null;
    try {
      fs.close();
    } 
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emitTuples() {
    // Limit emit rate
    if (counter >= maxTuplesPerWindow && evaluationPhase){
      return;
    }
    
    // Don't emit when training has ended but evaluation has not started
    if (!trainingPhase && !evaluationPhase){
      return;      
    }

    // Get a tuple from input and emit it
    String tuple = readEntity();
    if (tuple != null && tuple.trim().length() != 0) {
      lineOutputPort.emit(tuple);
      counter++;
    }
  }

  public long getMaxTuplesPerWindow() {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(long maxTuplesPerWindow) {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }


}
