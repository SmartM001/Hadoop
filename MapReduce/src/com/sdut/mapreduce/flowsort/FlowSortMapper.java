package com.sdut.mapreduce.flowsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	FlowBean bean = new FlowBean();
	Text v = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 获取一行数据
		String line = value.toString();
		
		// 2 截取字段
		String[] fields = line.split("\t");
		
		// 3 封装对象及获取电话号码
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		
		bean.set(upFlow, downFlow);
		v.set(fields[0]);
		
		// 4 写出去
		context.write(bean, v);
	}
}
