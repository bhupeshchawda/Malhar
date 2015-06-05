package com.datatorrent.lib.ml.classification;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This is the File line reader operator responsible for reading a file on HDFS
 * and sends a line as a tuple
 * 
 * @author
 *
 */
public class NBLineReader implements InputOperator {

	private static final Logger LOG = LoggerFactory
			.getLogger(NBLineReader.class);

	private long maxTuplesPerWindow;
	

	private final transient DefaultOutputPort<String> lineOutputPort = new DefaultOutputPort<String>();
	private transient BufferedReader br = null;
	private transient FileSystem fs = null;
	private NBConfig nbc = null;
	private boolean trainingPhase;
	private boolean evaluationPhase;
	private long counter = 0;

	public NBLineReader() {

	}

	public NBLineReader(NBConfig nbc) {
		this.nbc = nbc;
		if (nbc.isKFoldPartition()) {
			trainingPhase = true;
			evaluationPhase = false;
		}
	}

	protected String readEntity() {
		try {
			if (nbc.isKFoldPartition()) { // Both Training and Testing Phase is
											// needed
				if (trainingPhase) {
					String s = br.readLine();
					if (s != null) {
						return s;
					} else {
						initFileSystem();
						trainingPhase = false;
						LOG.info("File System Reseted and training Phase = false");
						return "@TRAINING_END";
					}
				}
				// else if(!evaluationPhase){
				// return "@WAIT";
				// }
				else {
					LOG.info("br is: {}", br.toString());
					String s = br.readLine();
					if (s != null) {
						return s;
					} else {
						return "";
					}
				}
			} else { // nbc.isOnlyTrain() || nbc.isOnlyEvaluate() - Only
						// Training Or Testing Phase. Send data only once.
				String s = br.readLine();
				if (s != null) {
					return s;
				} else {
					return "";
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException();
		}
	}

	public void endWindow() {
		if (!trainingPhase) { // Begin evaluation in next window
			// Check for file on Disk
			try {
				Path doneFile = new Path("/.done");
				Configuration conf = new Configuration();
				conf.addResource(new Path(
						"/usr/local/hadoop/etc/hadoop/core-site.xml"));
				FileSystem fs = FileSystem.get(conf);
				if (fs.exists(doneFile)) {
					evaluationPhase = true;
					LOG.info("Evaluation Phase starts");
					fs.delete(doneFile, true);
				}
			} catch (IOException e) {
				throw new RuntimeException();
			}
		}
	}

	@Override
	public void beginWindow(long arg0) {
		counter = 0;
	}

	@Override
	public void setup(OperatorContext arg0) {
		try {
			initFileSystem();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException();
		}

	}

	public void initFileSystem() throws IOException {
		Path filePath = new Path(nbc.getInputDataFile());
		Configuration conf = new Configuration();
//		conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		fs = FileSystem.get(conf);
		br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
		LOG.info("br is reset: {}", br.toString());
	}

	@Override
	public void teardown() {
		LOG.info("Tearing down :(");
		br = null;
		try {
			fs.close();
		} catch (IOException e) {
			throw new RuntimeException();
		}
	}

	@Override
	public void emitTuples() {
		if (counter >= maxTuplesPerWindow)
			return;
		if (!trainingPhase && !evaluationPhase)
			return;

		String tuple = readEntity();
		if (tuple != null && tuple.trim().length() != 0) {
			lineOutputPort.emit(tuple);
			counter++;
		}
	}

	public long getMaxTuplesPerWindow() {
		return maxTuplesPerWindow;
	}

	public void setMaxTuplesPerWindow(long maxTuplesPerWindow) {
		this.maxTuplesPerWindow = maxTuplesPerWindow;
	}


}
