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

public class NBModelAggregator<V extends NBModelStorage> extends BaseOperator
{

	private static final Logger LOG = LoggerFactory.getLogger(NBModelAggregator.class);

	NBModelStorage m;
	boolean changeInThisWindow = true;
	NBConfig nbc = null;
	int folds;
	NBModelStorage[] kFoldModels;
	boolean endOfInput = false;

	public NBModelAggregator(){
	}

	public NBModelAggregator(NBConfig nbc){
		this.nbc = nbc;
	}

	/**
	 * Input port for k-fold cross validation input. 
	 * The input is an intermediate model pointed to by the key i in the input entry.
	 * This key i is used in identifying which "fold" is to be tested in the model built using this. 
	 * Hence we merge the input intermediate model with the other k-1 models and store it as model i
	 */
	public final transient DefaultInputPort<MapEntry<Integer, NBModelStorage>> inTraining = 
			new DefaultInputPort<MapEntry<Integer, NBModelStorage>>() {

		@Override
		public void process(MapEntry<Integer, NBModelStorage> partModel) {
			if(partModel.getV() == null){
				endOfInput = true;
				return;
			}
			
			if(nbc.isKFoldPartition() && !partModel.getK().equals(-1)){
				for(int i=0;i<folds;i++){
					if(i != partModel.getK().intValue() && partModel.getV().instanceCount > 0){
						// Merge partModel[fold] in all Models except "fold"
						kFoldModels[i].merge(partModel.getV());	
						changeInThisWindow = true;
					}
				}
			}
			if(nbc.isOnlyTrain() && partModel.getK().equals(-1)){
				NBModelStorage md = partModel.getV();
				if(md.instanceCount > 0){
					m.merge(md);
					changeInThisWindow = true;
				}
			}
		}
	};

//	public final transient DefaultInputPort<Boolean> inDone = 
//			new DefaultInputPort<Boolean>() {
//
//				@Override
//				public void process(Boolean b) {
//					if(b.equals(true)){
//						outDone.emit(true);
//					}
//				}	
//	};

//	public final transient DefaultOutputPort<Boolean> outDone = 
//			new DefaultOutputPort<Boolean>();
	
	/**
	 * Output port which emits k-fold models as output in the end window method
	 */
	public final transient DefaultOutputPort<MapEntry<Integer, String>> outTraining = 
			new DefaultOutputPort<MapEntry<Integer, String>>();

	@Override
	public void setup(OperatorContext context) {
		super.setup(context);
		m = new NBModelStorage();

		if(nbc.isKFoldPartition()){
			folds = nbc.getNumFolds();
			kFoldModels = new NBModelStorage[folds];
			for(int i=0;i<folds;i++){
				kFoldModels[i] = new NBModelStorage();
			}
		}
	}

	/**
	 * Emit the final PMML representation of the model in a XML String
	 * In case of k-fold validation, emit the all k models
	 */
	@Override
	public void endWindow(){
		if(changeInThisWindow){
			changeInThisWindow = false;
			
			if(nbc.isKFoldPartition()){
				if(outTraining.isConnected()){
					for(int i=0;i<folds;i++){
						MapEntry<Integer, String> xmlModel = 
								new MapEntry<Integer, String>(i, kFoldModels[i].exportToPMML());
						outTraining.emit(xmlModel);
					}
				}
			}
			if(nbc.isOnlyTrain()){
				String s = m.exportToPMML();
				MapEntry<Integer, String> xmlModel = 
						new MapEntry<Integer, String>(-1, s);
				outTraining.emit(xmlModel);
			}
		}
		else{
			if(endOfInput){
				outTraining.emit(new MapEntry<Integer, String>(-1, null));
				endOfInput = false;
			}
		}
	}
}
