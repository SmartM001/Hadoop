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
		// ���󣺸���һƪ���£���������ÿ�����������ֵĴ���
		
		// 1 ��ȡ����
		String line = value.toString();
		
		// 2 �з�,�Զ��ַָ������зָ�
		StringTokenizer token = new StringTokenizer(line," ,.");
		
		// 3 д��ÿ������
		while(token.hasMoreTokens()){
			word.set(token.nextToken());
			context.write(word, new IntWritable(1));
		}
	}
}
