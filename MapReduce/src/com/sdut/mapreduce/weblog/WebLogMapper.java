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
		
		// 1 获取一行
		String line = value.toString();
		
		// 2 解析日志的方法
		boolean result = parseLog(line, context);
			
		// 3 判断是否合法
		if (!result) {
			return;
		}
		
		// 4合法的日志写出去
		context.write(value, NullWritable.get());
	}

	private boolean parseLog(String line, Context context) {
		// 1 截取
		String[] fields = line.split(" ");
		
		// 2 判断字段长度是否大于11
		if (fields.length > 11) {// 认为是合法的
			// 3 记录合法次数
			context.getCounter("map", "true").increment(1);
			
			return true;
		}else {// 认为是非法的
			// 4 记录不合法的次数
			
			context.getCounter("map", "false").increment(1);
			return false;
		}
	}
}
