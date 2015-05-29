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
 * Aggregates the output of NaiveBayesCounters, consolidates and converts the model data to PMML representation.
 * <p>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends ModelData<br>
 * <b>average</b>: emits String<br>
 * <br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * @displayName NaiveBayesModelAggregator
 * @category ml
 * @tags ml, naive bayes
 * @since 0.3.3
 */

public class NaiveBayesModelAggregator<V extends NaiveBayesModelStorage> extends BaseOperator
{

	private static final Logger LOG = LoggerFactory.getLogger(NaiveBayesModelAggregator.class);

	protected NaiveBayesModelStorage m;
	protected boolean changeInThisWindow = true;

	/**
	 * Input port that takes the intermediate model for a window of data processed by the NaiveBayesCounter operator
	 */
	public final transient DefaultInputPort<NaiveBayesModelStorage> data = new DefaultInputPort<NaiveBayesModelStorage>() {

		@Override
		public void process(NaiveBayesModelStorage md) {
			if(md.instanceCount > 0){
				m.merge(md);
				LOG.info("Merged. Total Instances = {}", m.instanceCount);
				changeInThisWindow = true;
			}
			else{
				LOG.info("0 Input instances.");
			}
		}
	};

	/**
	 * Output port that emits PMML model as a XML String
	 */
	public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

	@Override
	public void setup(OperatorContext context) {
		// TODO Auto-generated method stub
		super.setup(context);
		m = new NaiveBayesModelStorage();
	}

	/**
	 * Emit the final PMML representation of the model in a XML String
	 */
	@Override
	public void endWindow(){
		if(changeInThisWindow){
			String s = m.exportToPMML();
			output.emit(s);
			LOG.info("Emitted Instances = {}", m.instanceCount);
			changeInThisWindow = false;
		}

	}

}
