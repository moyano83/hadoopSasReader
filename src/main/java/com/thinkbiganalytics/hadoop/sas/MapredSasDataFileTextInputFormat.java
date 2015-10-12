package com.thinkbiganalytics.hadoop.sas;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.eobjects.metamodel.sas.SasReader;

public class MapredSasDataFileTextInputFormat extends FileInputFormat<IntWritable, Text>{

	@Override
	public RecordReader<IntWritable, Text> getRecordReader(
			InputSplit inputSplit, JobConf conf,
			Reporter arg2) throws IOException {
		SasRecordReader reader = new SasRecordReader();
		reader.initialize(inputSplit, conf);
		return reader;
	}

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	public class SasRecordReader implements RecordReader<IntWritable, Text> {

		private SasDefaultCallback sasReaderCallback = new SasDefaultCallback();
		/**
		 * The value to emit
		 */
		private Text value = new Text();
		/**
		 * The key to emit
		 */
		private IntWritable key = new IntWritable();
		/**
		 * Temporary file that holds the sas file
		 */
		private File tempFile;
		/**
		 * The original size of the list to read
		 */
		private int initialLoad = 0;
		public final static String FIELD_SEPARATOR = "\t";
		
		public void initialize(InputSplit inputSplit, JobConf context)
				throws IOException {
			FileSplit split = (FileSplit) inputSplit;
			FileSystem fs = FileSystem.get(context);
			FSDataInputStream in = null;
			tempFile = new File(System.getProperty("user.dir").concat(File.pathSeparator).concat(split.getPath().getName()));
			FileOutputStream bos = new FileOutputStream(tempFile);
			try {
				// Copy the file in hdfs to the local temp folder, reads it and
				// deletes the file on close
				in = fs.open(split.getPath());
				IOUtils.copyBytes(in, bos, context);
				SasReader reader = new SasReader(tempFile);
				reader.read(sasReaderCallback);
				//Stores the original number of elements to process
				initialLoad = sasReaderCallback.getResultObjectStack().size();
			} finally {
				IOUtils.closeStream(in);
				bos.close();
			}
		}

		public void close() throws IOException {
			tempFile.delete();
		}

		public IntWritable createKey() {
			return key;
		}

		public Text createValue() {
			return value;
		}

		public long getPos() throws IOException {
			return 0;
		}

		public float getProgress() throws IOException {
			return (sasReaderCallback.getResultObjectStack().size() / initialLoad ) * 100;
		}

		public boolean next(IntWritable arg0, Text arg1) throws IOException {
			if(!sasReaderCallback.getResultObjectStack().isEmpty()){
				key.set(initialLoad - sasReaderCallback.getResultObjectStack().size());

				Object[] rowObject = sasReaderCallback.getResultObjectStack().poll();
				value.set("");
				for (int i = 0;i<rowObject.length;i++) {
					value.append(rowObject[i].toString().getBytes(), 0, rowObject[i].toString().length());
					if(i!=(rowObject.length-1)){
						value.append(FIELD_SEPARATOR.getBytes(), 0, FIELD_SEPARATOR.length());
					}
				}
				
				return true;
			}
			return false;
		}

	}

}