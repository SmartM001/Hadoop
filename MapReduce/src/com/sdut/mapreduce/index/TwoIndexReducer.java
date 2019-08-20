package com.sdut.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text>{

	Text v = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		// ƴ��
		for(Text text :values){
			sb.append(text.toString() + "\t");
		}
		
		// �������value
		v.set(sb.toString());
		
		// д��
		context.write(key, v);
	}
}
