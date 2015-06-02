package com.datatorrent.lib.ml.classification;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;

/**
 * This is the Naive Bayes Output operator for overwriting output file on HDFS..
 * Custom created, since AbstractFileOutputOperator does not have the capability to overwrite files.
 *
 */
public class NBOutputPerWindowOperator extends BaseOperator {

	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(NBOutputPerWindowOperator.class);

	String fileName;
	String filePath;
	String tuple = "";
	boolean overwrite = false;
	NBConfig nbc = null;
	
	int folds;
	String[] xmlModels;

	public NBOutputPerWindowOperator(){
	}
	
	public NBOutputPerWindowOperator(NBConfig nbc){
		this.nbc = nbc;
	}
	
	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
		@Override
		public void process(String t) {
			tuple = t;			
		}
	};
	
	public final transient DefaultInputPort<MapEntry<Integer, String>> kFoldInput = 
			new DefaultInputPort<MapEntry<Integer, String>>() {
		@Override
		public void process(MapEntry<Integer, String> xmlModel) {
			int fold = xmlModel.getK();
			xmlModels[fold] = xmlModel.getV();
		}
	};


	@Override
	public void setup(OperatorContext context){
		if(nbc.isKFoldPartition()){
			folds = nbc.getNumFolds();
			xmlModels = new String[folds];
		}
	}

	@Override
	public void endWindow(){
		if(nbc.isKFoldPartition()){
			for(int i=0;i<xmlModels.length;i++){
				writeData(fileName+"."+i, xmlModels[i]);
			}
		}
		else{
			writeData(fileName, tuple);
		}
	}

	public void writeData(String fileName, String tuple){
		try {
			FileSystem fs = getFSInstance();
			Path writePath = new Path(filePath + Path.SEPARATOR + fileName);
			FSDataOutputStream out = null;
			if(overwrite){
				out = fs.create(writePath, overwrite);
			}
			else{
				if(fs.exists(writePath)){
					out = fs.append(writePath);
				}
				else{
					out = fs.create(writePath, overwrite);
				}
			}

			out.writeBytes(tuple);
			out.hflush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	public FileSystem getFSInstance() throws IOException
	{
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		return FileSystem.get(conf);
	}


}
