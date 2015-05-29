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

public class FileOutputPerWindowOperator extends BaseOperator {

	private static final Logger LOG = LoggerFactory.getLogger(FileOutputPerWindowOperator.class);

	String fileName;
	String filePath;
	String tuple = "";
	boolean overwrite = false;

	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {

		@Override
		public void process(String t) {
			tuple = t;			
		}
	};

	@Override
	public void setup(OperatorContext context){

	}

	@Override
	public void endWindow(){
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
		FileSystem fs = FileSystem.get(conf);
		return FileSystem.get(conf);
	}


}
