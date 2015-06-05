package com.datatorrent.lib.ml.classification;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class HashMapPerformance {
	
	public HashMap<HashMap<HashMap<String, Integer>, Integer>, Integer> hm = new HashMap<HashMap<HashMap<String,Integer>,Integer>, Integer>();
	public Map<String, Integer>[][] hm2d = (Map<String, Integer>[][]) new Map[10][1];
	
	
	@Test
	public void test() {


	}

}
