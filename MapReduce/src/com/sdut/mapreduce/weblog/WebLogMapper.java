package com.sdut.mapreduce.weblog;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// 1 ��ȡһ��
		String line = value.toString();
		
		// 2 ������־�ķ���
		boolean result = parseLog(line, context);
			
		// 3 �ж��Ƿ�Ϸ�
		if (!result) {
			return;
		}
		
		// 4�Ϸ�����־д��ȥ
		context.write(value, NullWritable.get());
	}

	private boolean parseLog(String line, Context context) {
		// 1 ��ȡ
		String[] fields = line.split(" ");
		
		// 2 �ж��ֶγ����Ƿ����11
		if (fields.length > 11) {// ��Ϊ�ǺϷ���
			// 3 ��¼�Ϸ�����
			context.getCounter("map", "true").increment(1);
			
			return true;
		}else {// ��Ϊ�ǷǷ���
			// 4 ��¼���Ϸ��Ĵ���
			
			context.getCounter("map", "false").increment(1);
			return false;
		}
	}
}
