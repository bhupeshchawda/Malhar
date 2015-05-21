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
package com.datatorrent.lib.ml;

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
 * 1. Input string is in CSV format, with features followed by the class label at the end. 
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

public class NaiveBayesCounter extends BaseOperator
{
	/**
	 * Object of type ModelData which will contain the intermediate model to be emitted at the end of the window
	 */
	protected transient ModelData m;

	/**
	 * Input port that takes a String.
	 */
	public final transient DefaultInputPort<String> data = new DefaultInputPort<String>() {

		@Override
		public void process(String in) {
			String[] parts = (String[]) in.toString().split(",");
			m.updateModel(parts);
		}
	};
	
	/**
	 * Output port that emits an object of type ModelData
	 */
	public final transient DefaultOutputPort<ModelData> counterOutput = new DefaultOutputPort<ModelData>();

	/**
	 * Setup method for the operator. Initializes the ModelData object.
	 */
	@Override
	public void setup(OperatorContext context) {
		// TODO Auto-generated method stub
		super.setup(context);
		m = new ModelData();
	}
	
	/**
	 * Emit ModelData m
	 */
	@Override
	public void endWindow(){
		counterOutput.emit(m);
	}

	/**
	 * Clear the ModelData object m
	 */
	public void beginWindow(long windowId){
		m.clear();
	}
	
}
