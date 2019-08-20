package com.sdut.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 1 获取一行
		String line = value.toString();

		// 2 截取
		String[] fields = line.split(" ");

		// 3 获取文件名称
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		String name = inputSplit.getPath().getName();

		// 4 拼接
		for (int i = 0; i < fields.length; i++) {

			k.set(fields[i] + "--" + name);

			// 5 输出
			context.write(k, new IntWritable(1));
		}
	}
}
