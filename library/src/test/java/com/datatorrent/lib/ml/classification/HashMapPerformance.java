package com.datatorrent.lib.ml.classification;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

public class HashMapPerformance {

  public HashMap<String, HashMap<String, HashMap<String, Integer>>> hm = new HashMap<String, HashMap<String,HashMap<String,Integer>>>();
  public HashMap<String, Integer>[][] hm2d = (HashMap<String, Integer>[][]) new HashMap[28][2];

  @Test
  public void testPerformance() throws IOException{
    long startHm = System.currentTimeMillis();
    populateHm();
    long stopHm = System.currentTimeMillis();
    System.out.println("HashMap:"+ (stopHm - startHm));

    long startHm2D = System.currentTimeMillis();
    populateHm2D();
    long stopHm2D = System.currentTimeMillis();
    System.out.println("HashMap2D:"+ (stopHm2D - startHm2D));
    
    Assert.assertEquals("Random Check", hm.get("0").get("0").get("0"), hm2d[0][0].get("0"));
    Assert.assertEquals("Random Check", hm.get("0").get("1").get("0"), hm2d[0][0].get("1"));
    Assert.assertEquals("Random Check", hm.get("15").get("-1").get("0"), hm2d[15][0].get("-1"));
    Assert.assertEquals("Random Check", hm.get("15").get("-1").get("0"), hm2d[15][0].get("-1"));
    Assert.assertEquals("Random Check", hm.get("27").get("0").get("0"), hm2d[27][0].get("0"));
    Assert.assertEquals("Random Check", hm.get("27").get("1").get("0"), hm2d[27][0].get("1"));


  }

  public void populateHm() throws IOException{
    BufferedReader br = null;
    long count = 0;
    try{
      br = new BufferedReader(new FileReader(new File("/home/bhupesh/docs/MachineLearning/datasets/TEST")));
      String s = "";
      while((s = br.readLine()) != null){
        count++;
        if(count%1000000 == 0){
          System.out.println("Processing: "+count);
        }
        String[] parts = s.split(",");
        String c = parts[parts.length-1];
        for(int i=0;i<parts.length-1;i++){
          String fn = i+"";
          String fv = parts[i];
          if(hm.containsKey(fn)){
            HashMap<String, HashMap<String, Integer>> hfn = hm.get(fn);
            if(hfn.containsKey(fv)){
              HashMap<String, Integer> hfv = hm.get(fn).get(fv);
              if(hfv.containsKey(c)){
                hfv.put(c,hfv.get(c)+1);
              }
              else{
                hfv.put(c, 1);
              }
            }
            else{
              HashMap<String, Integer> hfv = new HashMap<String, Integer>();
              hfv.put(c, 1);
              hfn.put(fv, hfv);
            }
          }
          else{
            HashMap<String, HashMap<String, Integer>> hfn = new HashMap<String, HashMap<String,Integer>>();
            HashMap<String, Integer> hfv = new HashMap<String, Integer>();
            hfv.put(c, 1);
            hfn.put(fv, hfv);
            hm.put(fn, hfn);
          }
        }
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
    finally{
      br.close();  
    }
  }
  
  
  public void populateHm2D() throws IOException {
    BufferedReader br = null;
    long count = 0;
    try{
      br = new BufferedReader(new FileReader("/home/bhupesh/docs/MachineLearning/datasets/TEST"));
      String s = "";
      while((s = br.readLine()) != null){
        count++;
        if(count%1000000 == 0){
          System.out.println("Processing: "+count);
        }
        String[] parts = s.split(",");
        int c = Integer.parseInt(parts[parts.length-1]);
        for(int i=0;i<parts.length-1;i++){
          if(hm2d[i][c] == null){
            HashMap<String, Integer> hm = new HashMap<String, Integer>();
            hm.put(parts[i], 1);
            hm2d[i][c] = hm;
          }
          else{
            HashMap<String, Integer> hm = hm2d[i][c];
            if(hm.containsKey(parts[i])){
              hm.put(parts[i], hm.get(parts[i])+1);
            }
            else{
              hm.put(parts[i], 1);
            }
          }
        }  
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
    finally{
      br.close();  
    }    
  }
}
