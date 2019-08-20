package com.sdut.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEYIN:输入数据的key
 * VALUEIN:输入数据的value
 * 
 * KEYOUT:输出数据的key
 * VALUEOUT:输出数据的out
 * @author Ming
 *
 */

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 获取一行数据
		String line = value.toString();
		
		// 2 获取每一个单词
		String[] words = line.split(" ");
		
		// 3 输出每一个单词
		for(String word:words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
