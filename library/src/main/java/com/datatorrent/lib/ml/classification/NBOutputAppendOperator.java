package com.datatorrent.lib.ml.classification;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * This is the Naive Bayes Output operator which writes the input string to a file on HDFS.
 * This extends the AbstractFileOutputOperator and hence, only has the capability to append data, and cannot overwrite
 *
 */
public class NBOutputAppendOperator extends AbstractFileOutputOperator<String> {

	String fileName = "";

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	@Override
	protected String getFileName(String tuple) {
		return fileName;
	}

	@Override
	protected byte[] getBytesForTuple(String tuple) {
		return tuple.getBytes();
	}

	@Override
	protected FileSystem getFSInstance() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		return FileSystem.get(conf);
	}
	
}
