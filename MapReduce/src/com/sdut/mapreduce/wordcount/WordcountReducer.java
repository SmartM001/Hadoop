package com.sdut.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	protected void reduce(Text key, Iterable<IntWritable> values, 
			Context context) throws java.io.IOException ,InterruptedException {
		// 1 统计所有单词的个数
		int count=0;
		for(IntWritable value:values) {
			count += value.get();
		}
		
		// 2 输出所有单词个数
		context.write(key, new IntWritable(count));
	}
}
