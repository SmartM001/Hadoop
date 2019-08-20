package cn.edu.sdut.mapreduce.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class wordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private Text word = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 需求：给定一篇文章，计算其中每个单词所出现的次数
		
		// 1 获取数据
		String line = value.toString();
		
		// 2 切分,以多种分隔符进行分割
		StringTokenizer token = new StringTokenizer(line," ,.");
		
		// 3 写出每个单词
		while(token.hasMoreTokens()){
			word.set(token.nextToken());
			context.write(word, new IntWritable(1));
		}
	}
}
