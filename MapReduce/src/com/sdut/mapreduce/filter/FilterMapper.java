package com.sdut.mapreduce.filter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	private Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// 获取一行
		String line = value.toString();
		
		k.set(line);
		
		// 写出
		context.write(k, NullWritable.get());
	}
}
