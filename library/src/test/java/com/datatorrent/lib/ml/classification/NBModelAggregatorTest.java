//package com.datatorrent.lib.ml.classification;
//
//import java.io.ByteArrayInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.nio.charset.StandardCharsets;
//
//import javax.xml.parsers.DocumentBuilder;
//import javax.xml.parsers.DocumentBuilderFactory;
//import javax.xml.parsers.ParserConfigurationException;
//
//import org.junit.Assert;
//import org.junit.Test;
//import org.w3c.dom.Document;
//import org.w3c.dom.Element;
//import org.w3c.dom.Node;
//import org.w3c.dom.NodeList;
//import org.xml.sax.SAXException;
//
//import com.datatorrent.lib.testbench.CollectorTestSink;
//
///**
// * Functional test for {@link NBModelAggregator}
// * 
// * Creates two windows of tuples and verifies the node output as well the intermediate state after merging multiple inputs.
// * @author bhupesh
// *
// */
//
//public class NBModelAggregatorTest {
//
//	@Test
//	@SuppressWarnings({ "rawtypes", "unchecked" })
//	public void testNodeProcessing() throws ParserConfigurationException, SAXException, IOException{
//		NBConfig nbc = new NBConfig();
//		NBModelAggregator<NBModelStorage> nbAggOper = new NBModelAggregator<NBModelStorage>(nbc);
//		CollectorTestSink nbSink = new CollectorTestSink();
//
//		nbAggOper.output.setSink(nbSink);
//
//		nbAggOper.setup(null);
//
//
//		//Window 0
//		nbAggOper.beginWindow(0);
//		NBModelStorage modelDataInput0 = prepareInputWindow0();
//		nbAggOper.data.process(modelDataInput0);
//		nbAggOper.endWindow();
//
//		Assert.assertEquals("Number of tuples at end of Window 0", 1, nbSink.collectedTuples.size());
//
//		//Window 1
//		nbAggOper.beginWindow(1);
//		NBModelStorage modelDataInput1 = prepareInputWindow1();
//		nbAggOper.data.process(modelDataInput1);
//		nbAggOper.endWindow();
//
//		Assert.assertEquals("Number of tuples at end of Window 1", 2, nbSink.collectedTuples.size());
//
//
//		verifyXML(nbSink.collectedTuples.get(1).toString());
//	}
//
//	/**
//	 * Helper method to verify the output XML string and the data in the XML schema.
//	 * 
//	 * @param xmlString
//	 * @throws ParserConfigurationException
//	 * @throws SAXException
//	 * @throws IOException
//	 */
//	public void verifyXML(String xmlString){
//		try{
//			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
//			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//			InputStream inStream = new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8));
//			Document doc = dBuilder.parse(inStream);
//
//			NodeList bi = doc.getElementsByTagName("BayesInput");
//			for(int i=0;i<bi.getLength();i++){
//				Node n = bi.item(i);
//				if(n.getNodeType() == Node.ELEMENT_NODE){
//					String featureName = ((Element)n).getAttribute("fieldName");
//					NodeList pc = ((Element)n).getElementsByTagName("PairCounts");
//					for(int j=0;j<pc.getLength();j++){
//						Node m = pc.item(j);
//						if(m.getNodeType() == Node.ELEMENT_NODE){
//							String featureValue = ((Element)m).getAttribute("value");
//							NodeList tvc = ((Element)m).getElementsByTagName("TargetValueCount");
//							for(int k=0;k<tvc.getLength();k++){
//								Node o = tvc.item(k);
//								if(o.getNodeType() == Node.ELEMENT_NODE){
//									String c = ((Element)o).getAttribute("value");
//									String count = ((Element)o).getAttribute("count");
//
//									//Check few Counts
//									if(featureName.equals("0") && featureValue.equals("1") && c.equals("A"))	Assert.assertEquals("4.0", count);
//									if(featureName.equals("0") && featureValue.equals("11") && c.equals("A"))	Assert.assertEquals("2.0", count);
//									if(featureName.equals("0") && featureValue.equals("111") && c.equals("A"))	Assert.assertEquals("2.0", count);
//									if(featureName.equals("1") && featureValue.equals("2") && c.equals("A"))	Assert.assertEquals("6.0", count);
//									if(featureName.equals("1") && featureValue.equals("222") && c.equals("A"))	Assert.assertEquals("2.0", count);
//									if(featureName.equals("0") && featureValue.equals("1") && c.equals("D"))	Assert.assertEquals("2.0", count);
//									if(featureName.equals("0") && featureValue.equals("11") && c.equals("D"))	Assert.assertEquals("1.0", count);
//								}
//							}
//						}					
//					}
//				}
//			}
//		}catch(Exception e){
//			throw new RuntimeException();
//		}
//	} 
//
//	/**
//	 * Helper method to create the input in Window 0
//	 * @return
//	 */
//	public NBModelStorage prepareInputWindow0(){
//		NBModelStorage m = new NBModelStorage();
//		m.updateModel(NBInputReader.parseAsCsv("1,2,3,44,55,A"));
//		m.updateModel(NBInputReader.parseAsCsv("11,2,333,4,555,A"));
//		m.updateModel(NBInputReader.parseAsCsv("1,2,3,4,5,A"));
//		m.updateModel(NBInputReader.parseAsCsv("111,222,3,44,55,A"));
//		m.updateModel(NBInputReader.parseAsCsv("1,22,3,4,5,B"));
//		m.updateModel(NBInputReader.parseAsCsv("1,2,333,444,5,B"));
//		m.updateModel(NBInputReader.parseAsCsv("111,2,3,4,5,B"));
//		m.updateModel(NBInputReader.parseAsCsv("1,2,33,4,5,B"));
//		m.updateModel(NBInputReader.parseAsCsv("1,2,3,4,5,C"));
//		m.updateModel(NBInputReader.parseAsCsv("11,22,3,4,5,C"));
//		return m;
//	}
//
//	/**
//	 * Helper method to create the input in Window 1
//	 * @return
//	 */
//	public NBModelStorage prepareInputWindow1(){
//		NBModelStorage m = new NBModelStorage();
//		//Add more samples for existing Class - A
//		m.updateModel(NBInputReader.parseAsCsv("1,2,3,44,55,A"));
//		m.updateModel(NBInputReader.parseAsCsv("11,2,333,4,555,A"));
//		m.updateModel(NBInputReader.parseAsCsv("1,2,3,4,5,A"));
//		m.updateModel(NBInputReader.parseAsCsv("111,222,3,44,55,A"));
//		//Add a new Class - D
//		m.updateModel(NBInputReader.parseAsCsv("1,2,3,44,55,D"));
//		m.updateModel(NBInputReader.parseAsCsv("11,2,333,4,555,D"));
//		m.updateModel(NBInputReader.parseAsCsv("1,2,3,4,5,D"));
//		return m;
//	}
//
//
//}
