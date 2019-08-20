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
		// 1 ��ȡһ��
		String line = value.toString();

		// 2 ��ȡ
		String[] fields = line.split(" ");

		// 3 ��ȡ�ļ�����
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		String name = inputSplit.getPath().getName();

		// 4 ƴ��
		for (int i = 0; i < fields.length; i++) {

			k.set(fields[i] + "--" + name);

			// 5 ���
			context.write(k, new IntWritable(1));
		}
	}
}
