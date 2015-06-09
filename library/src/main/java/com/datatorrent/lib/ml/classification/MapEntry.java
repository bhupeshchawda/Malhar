package com.datatorrent.lib.ml.classification;

import java.io.Serializable;

/**
 * 
 * This is a utility class for storing key value pairs and has a default constructor for kyro serialization.
 * @author 
 *
 * @param <K>
 * @param <V>
 */
public class MapEntry<K,V> implements Serializable{

  private static final long serialVersionUID = 8482795977827948626L;
  
  K k;
  V v;
  
  public MapEntry(){
    
  }
  
  public MapEntry(K k, V v){
    this.k = k;
    this.v = v;
  }
  
  public K getK() {
    return k;
  }
  public void setK(K k) {
    this.k = k;
  }
  public V getV() {
    return v;
  }
  public void setV(V v) {
    this.v = v;
  }
  
  
  
}
