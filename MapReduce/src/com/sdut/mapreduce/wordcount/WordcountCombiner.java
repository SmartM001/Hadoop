package com.sdut.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		// ͳ������
		int count = 0;
		for(IntWritable value : values){
			count += value.get();
		}
		
		// д��
		context.write(key, new IntWritable(count));
	}
}
