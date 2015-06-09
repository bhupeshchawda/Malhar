package com.datatorrent.lib.ml.classification;

import org.junit.Assert;
import org.junit.Test;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Functional test for {@link NBCounter}
 * Creates a window of 10 tuples and verifies the corresponding output.
 * @author bhupesh
 *
 */
public class NBCounterTest {

  /**
   * Test processing of the NaiveBayesCounter operator node
   */
  @Test
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testNodeProcessing(){
    //Schema: {a,b,c,d,Class}
    int numAttributes = 5;
    int numClasses = 3;
    int numInstances = 10;

    NBConfig nbc = new NBConfig(); 
    nbc.setKFoldPartition(true);
    nbc.setNumAttributes(numAttributes);
    nbc.setNumClasses(numClasses);
    NBCounter nbOper = new NBCounter(nbc);
    CollectorTestSink nbSink = new CollectorTestSink();
    
    nbOper.outTraining.setSink(nbSink);
    

    nbOper.setup(null);
    nbOper.beginWindow(0);
    
    nbOper.inTraining.process(new MapEntry<Integer, String[]>(0,NBInputReader.parseAsCsv("1,2,3,44,55,0")));
    nbOper.inTraining.process(new MapEntry<Integer, String[]>(0,NBInputReader.parseAsCsv("11,2,333,4,555,0")));
    nbOper.inTraining.process(new MapEntry<Integer, String[]>(0,NBInputReader.parseAsCsv("1,2,3,4,5,0")));
    nbOper.inTraining.process(new MapEntry<Integer, String[]>(0,NBInputReader.parseAsCsv("111,222,3,44,55,0")));
    nbOper.inTraining.process(new MapEntry<Integer, String[]>(0,NBInputReader.parseAsCsv("1,22,3,4,5,1")));
    nbOper.inTraining.process(new MapEntry<Integer, String[]>(0,NBInputReader.parseAsCsv("1,2,333,444,5,1")));
    nbOper.inTraining.process(new MapEntry<Integer, String[]>(0,NBInputReader.parseAsCsv("111,2,3,4,5,1")));
    nbOper.inTraining.process(new MapEntry<Integer, String[]>(0,NBInputReader.parseAsCsv("1,2,33,4,5,2")));    
    nbOper.inTraining.process(new MapEntry<Integer, String[]>(0,NBInputReader.parseAsCsv("1,2,3,4,5,2")));
    nbOper.inTraining.process(new MapEntry<Integer, String[]>(0,NBInputReader.parseAsCsv("11,22,3,4,5,2")));

    nbOper.endWindow();
    
    Assert.assertEquals(1, nbSink.collectedTuples.size());
    NBModelStorage m =  ((MapEntry<Integer, NBModelStorage>)nbSink.collectedTuples.get(0)).getV();
    Assert.assertEquals(numInstances, m.instanceCount);
    
    for(int i=0;i<numAttributes;i++){
      for(int j=0;j<numClasses;j++){
        System.out.print(m.featureTableCategorical[i][j] + "\t");
      }
      System.out.println();
    }
  }


}
