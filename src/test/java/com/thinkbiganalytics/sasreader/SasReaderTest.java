package com.thinkbiganalytics.sasreader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import com.thinkbiganalytics.hadoop.sas.SasDataFileTextInputFormat;

public class SasReaderTest{

	private final static String TEST_FILENAME = "airline.sas7bdat";
	
	private String rootExecutionFolder = getClass().getClassLoader().getResource(".").getPath();
	
	private Path outDir = new Path("test/sas/output/");
	
	private Path inDir = new Path("test/sas/input/");
	
	private LocalFileSystem filesystem;
	
	private Configuration conf = new Configuration(false);
	
	@Before
	public void init() throws IOException, URISyntaxException{
		conf.set("fs.default.name", "file:///");
		filesystem = LocalFileSystem.getLocal(conf);
		filesystem.mkdirs(inDir);
		filesystem.mkdirs(outDir);
		filesystem.copyFromLocalFile(new Path(rootExecutionFolder.concat(TEST_FILENAME)), inDir);
		
		TestCase.assertNotNull(filesystem.getFileStatus(new Path(inDir, TEST_FILENAME)));
	}
	
	@Test
	public void sasInputTest() throws Exception{
		Configuration conf = new Configuration(false);
		conf.set("fs.defaultFS", "file:///");

		File testFile = new File(rootExecutionFolder.concat(TEST_FILENAME));
		Path path = new Path(testFile.getAbsoluteFile().toURI());
		FileSplit split = new FileSplit(path, 0, testFile.length(), null);

		InputFormat<IntWritable, Text> inputFormat = ReflectionUtils.newInstance(SasDataFileTextInputFormat.class, conf);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		RecordReader<IntWritable, Text> reader = inputFormat.createRecordReader(split, context);

		int count=0;
		while(reader.nextKeyValue()){
			TestCase.assertEquals(count, reader.getCurrentKey().get());
			TestCase.assertNotNull(reader.getCurrentValue());
			
			System.out.println(reader.getCurrentKey() + "--->" + reader.getCurrentValue());
			count++;
		};
		TestCase.assertEquals(33, count);
	}

}
