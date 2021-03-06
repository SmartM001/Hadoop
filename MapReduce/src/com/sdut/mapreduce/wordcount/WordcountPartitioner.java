package com.sdut.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordcountPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {

		// 1 ��ȡ��������ĸ
		String firstWord = key.toString().substring(0, 1);
		char[] charArray = firstWord.toCharArray();
		int result = charArray[0];

		if (result % 2 == 0) {
			return 0;
		} else {
			return 1;
		}
	}

}
