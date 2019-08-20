package com.sdut.mapreduce.filter;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterOutputformat extends FileOutputFormat<Text, NullWritable>{

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		// 创建一个RecordWriter
		return new FilterRecordWriter(job);
	}
}
