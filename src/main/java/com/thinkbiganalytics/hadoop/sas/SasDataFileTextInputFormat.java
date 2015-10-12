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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.eobjects.metamodel.sas.SasReader;

public class SasDataFileTextInputFormat extends FileInputFormat<IntWritable, Text>{


	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit inputSplit,
			TaskAttemptContext conf) throws IOException, InterruptedException {
		SASRecordReader sasRecordReader = new SASRecordReader();
		sasRecordReader.initialize(inputSplit, conf);
		return sasRecordReader; 
	}
	

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}


	private static class SASRecordReader extends RecordReader<IntWritable, Text>{
		
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
		 * Stores the lenght of the file read
		 */
		private int initialLoad = 0;
		
		public final static String FIELD_SEPARATOR = "\t";

		@Override
		public IntWritable getCurrentKey() throws IOException,InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit)arg0;
            FileSystem  fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream in = null; 
            tempFile = new File(System.getProperty("user.dir").concat(File.separator).concat(split.getPath().getName()));
            FileOutputStream bos = new FileOutputStream(tempFile);
            try {
            		//Copy the file in hdfs to the local temp folder, reads it and deletes the file on close
                    in = fs.open( split.getPath());
                    IOUtils.copyBytes(in, bos,context.getConfiguration());
                    SasReader reader = new SasReader(tempFile);
                    reader.read(sasReaderCallback);
                    
                    initialLoad = sasReaderCallback.getResultObjectStack().size();
            } finally {
                    IOUtils.closeStream(in);
                    bos.close();
            }
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
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

		@Override
		public void close() throws IOException {
			tempFile.delete();
		}
		
		@Override
		public float getProgress() throws IOException, InterruptedException {
			if(initialLoad ==0){
				return 0;
			}
			//Don't really care about the decimals
			return sasReaderCallback.getResultObjectStack().size()/initialLoad * 100;
		}
	}

}
