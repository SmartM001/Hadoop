package com.sdut.mapreduce.wholefile;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputformat extends FileInputFormat<NullWritable, BytesWritable>{

	// �Ƿ�����и�
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		
		return false;
	}
	
	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		WholeRecordReader reader = new WholeRecordReader();
		
		// ���ó�ʼ������
		reader.initialize(split, context);
		
		return reader;
	}

}
