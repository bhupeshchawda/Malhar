/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.ml.classification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

/**
 *
 * Emits an object of type ModelData at the end of window. 
 * This object consists of the model information for the training data received within that window.
 * <p>
 * Assumes the following - 
 * 1. Input is a String[], with features followed by the class label at the last index of the array. 
 * 2. The optype and datatype of the features is set to Continuous and Double for now.
 * These assumptions can be relaxed by accepting an input header from the user describing the format and the datatypes in the input data.
 * <p>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends String<br>
 * <b>counterOutput</b>: emits ModelData<br>
 * <br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * @displayName NaiveBayesCounter
 * @category ml
 * @tags ml, naive bayes
 * @since 0.3.3
 */

public class NBCounter extends BaseOperator
{
	private static final Logger LOG = LoggerFactory.getLogger(NBCounter.class);

	/**
	 * Object of type ModelData which will contain the intermediate model to be emitted at the end of the window
	 */
	transient NBModelStorage m;
	transient int folds;
	transient NBModelStorage[] kFoldModels;
	boolean changedInWindow = false;
	NBConfig nbc = null;

	public NBCounter(){
	}

	public NBCounter(NBConfig nbc){
		this.nbc = nbc;
	}

	/**
	 * Input port that takes a String Array
	 */
	public final transient DefaultInputPort<String[]> input = 
			new DefaultInputPort<String[]>() {

				@Override
				public void process(String[] in) {
					m.updateModel(in);
					changedInWindow = true;
				}
			};

			public final transient DefaultInputPort<MapEntry<Integer, String[]>> kFoldInput = 
					new DefaultInputPort<MapEntry<Integer, String[]>>() {

				@Override
				public void process(MapEntry<Integer, String[]> instance) {
					kFoldModels[instance.getK()].updateModel(instance.getV());
					changedInWindow = true;
				}
			};

			/**
			 * Output port that emits an object of type NBModelStorage
			 */
			public final transient DefaultOutputPort<NBModelStorage> output = new DefaultOutputPort<NBModelStorage>();

			/**
			 * Output port that emits an object of type Entry<Integer, NBModelStorage>
			 */
			public final transient DefaultOutputPort<MapEntry<Integer, NBModelStorage>> kFoldOutput = 
					new DefaultOutputPort<MapEntry<Integer, NBModelStorage>>();

			/**
			 * Setup method for the operator. Initializes the NBModelStorage object.
			 */
			@Override
			public void setup(OperatorContext context) {
				m = new NBModelStorage();
				folds = nbc.getNumFolds();
				kFoldModels = new NBModelStorage[folds];
				for(int i=0;i<folds;i++){
					kFoldModels[i] = new NBModelStorage();
				}
			}

			/**
			 * Emit ModelData m
			 */
			@Override
			public void endWindow(){
				if(changedInWindow){
					changedInWindow = false;
					if(nbc.isKFoldPartition()){
						if(kFoldOutput.isConnected()){
							for(int i=0;i<folds;i++){
								MapEntry<Integer, NBModelStorage> intermediateModel = 
										new MapEntry<Integer, NBModelStorage>(i, kFoldModels[i]);
								kFoldOutput.emit(intermediateModel);
							}
						}
						else{
							LOG.info("No operator connected to kFoldOutput port.");
						}
					}
					else{
						output.emit(m);
						changedInWindow = false;
						LOG.info("Emitted. Instances = {}",m.instanceCount);
					}
				}
			}

			/**
			 * Clear the ModelData object m
			 */
			public void beginWindow(long windowId){
				m.clear();
				if(nbc.isKFoldPartition()){
					for(int i=0;i<folds;i++){
						kFoldModels[i].clear();
					}
				}
			}

}
