package com.sdut.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	FlowBean bean = new FlowBean();
	Text k = new Text();
	
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException ,InterruptedException {
		// 1 获取一行数据
		String line = value.toString();
		
		// 2 截取字段
		String[] fields = line.split("\t");
		
		// 3 封装bean对象以及获取电话号码
		String phoneNum = fields[1];
		long upFlow = Long.parseLong(fields[fields.length-3]);
		long downFlow = Long.parseLong(fields[fields.length-2]);
		
		bean.set(upFlow, downFlow);
		k.set(phoneNum);
		
		// 4 写出去
		context.write(k, bean);
		
	}
}
