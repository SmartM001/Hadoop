package com.sdut.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEYIN:�������ݵ�key
 * VALUEIN:�������ݵ�value
 * 
 * KEYOUT:������ݵ�key
 * VALUEOUT:������ݵ�out
 * @author Ming
 *
 */

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 ��ȡһ������
		String line = value.toString();
		
		// 2 ��ȡÿһ������
		String[] words = line.split(" ");
		
		// 3 ���ÿһ������
		for(String word:words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
