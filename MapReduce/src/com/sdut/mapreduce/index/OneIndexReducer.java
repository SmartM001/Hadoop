package com.sdut.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		// 累加求和
		int count = 0;
		for(IntWritable value:values){
			count += value.get();
		}
		
		// 写出去
		context.write(key, new IntWritable(count));
	}

}
