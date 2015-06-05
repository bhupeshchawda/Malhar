//package com.datatorrent.lib.ml.classification;
//
//import org.junit.Assert;
//import org.junit.Test;
//import com.datatorrent.lib.ml.classification.*;
//import com.datatorrent.lib.testbench.CollectorTestSink;
//
///**
// * Functional test for {@link NBCounter}
// * Creates a window of 10 tuples and verifies the corresponding output.
// * @author bhupesh
// *
// */
//public class NBCounterTest {
//
//	/**
//	 * Test processing of the NaiveBayesCounter operator node
//	 */
//	@Test
//	@SuppressWarnings({ "rawtypes", "unchecked" })
//	public void testNodeProcessing(){
//		NBConfig nbc = new NBConfig(); 
//		NBCounter nbOper = new NBCounter(nbc);
//		CollectorTestSink nbSink = new CollectorTestSink();
//		
//		nbOper.output.setSink(nbSink);
//		
//		//Schema: {a,b,c,d,Class}
//		int numAttributes = 5;
//		int numClasses = 3;
//		int numInstances = 10;
//
//		nbOper.setup(null);
//		nbOper.beginWindow(0);
//		
//		nbOper.input.process(NBInputReader.parseAsCsv("1,2,3,44,55,A"));
//		nbOper.input.process(NBInputReader.parseAsCsv("11,2,333,4,555,A"));
//		nbOper.input.process(NBInputReader.parseAsCsv("1,2,3,4,5,A"));
//		nbOper.input.process(NBInputReader.parseAsCsv("111,222,3,44,55,A"));
//		nbOper.input.process(NBInputReader.parseAsCsv("1,22,3,4,5,B"));
//		nbOper.input.process(NBInputReader.parseAsCsv("1,2,333,444,5,B"));
//		nbOper.input.process(NBInputReader.parseAsCsv("111,2,3,4,5,B"));
//		nbOper.input.process(NBInputReader.parseAsCsv("1,2,33,4,5,B"));		
//		nbOper.input.process(NBInputReader.parseAsCsv("1,2,3,4,5,C"));
//		nbOper.input.process(NBInputReader.parseAsCsv("11,22,3,4,5,C"));
//		
//		nbOper.endWindow();
//		
//		Assert.assertEquals(1, nbSink.collectedTuples.size());
//		NBModelStorage m = (NBModelStorage) nbSink.collectedTuples.get(0);
//		Assert.assertEquals(numInstances, m.instanceCount);
//		Assert.assertEquals(numClasses, m.classCounts.keySet().size());
//		Assert.assertEquals(numAttributes, m.featureTableCategorical.keySet().size());
//
//		Assert.assertEquals("{A=2, B=3, C=1}", m.featureTableCategorical.get("0").get("1").toString());
//		Assert.assertEquals("{A=1, C=1}", m.featureTableCategorical.get("0").get("11").toString());
//		Assert.assertEquals("{A=1, B=1}", m.featureTableCategorical.get("0").get("111").toString());
//		Assert.assertEquals("{A=3, B=3, C=1}", m.featureTableCategorical.get("1").get("2").toString());
//		Assert.assertEquals("{B=1, C=1}", m.featureTableCategorical.get("1").get("22").toString());
//		Assert.assertEquals("{A=1}", m.featureTableCategorical.get("1").get("222").toString());
//		Assert.assertEquals("{A=3, B=2, C=2}", m.featureTableCategorical.get("2").get("3").toString());
//		Assert.assertEquals("{B=1}", m.featureTableCategorical.get("2").get("33").toString());
//		Assert.assertEquals("{A=1, B=1}", m.featureTableCategorical.get("2").get("333").toString());
//		Assert.assertEquals("{A=2, B=3, C=2}", m.featureTableCategorical.get("3").get("4").toString());
//		Assert.assertEquals("{A=2}", m.featureTableCategorical.get("3").get("44").toString());
//		Assert.assertEquals("{B=1}", m.featureTableCategorical.get("3").get("444").toString());
//		Assert.assertEquals("{A=1, B=4, C=2}", m.featureTableCategorical.get("4").get("5").toString());
//		Assert.assertEquals("{A=2}", m.featureTableCategorical.get("4").get("55").toString());
//		Assert.assertEquals("{A=1}", m.featureTableCategorical.get("4").get("555").toString());
//			
//	}
//
//
//}
