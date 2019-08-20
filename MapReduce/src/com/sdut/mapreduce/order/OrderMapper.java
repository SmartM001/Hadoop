package com.sdut.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
	
	OrderBean bean = new OrderBean();
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)throws IOException, InterruptedException {
		// 1 ������
		String line = value.toString();
		
		// 2 �и�����
		String[] fields = line.split("\t");
		
		// 3 ��װbean����
		bean.setOrderId(fields[0]);
		bean.setPrice(Double.parseDouble(fields[2]));
		
		// 4 д��
		context.write(bean, NullWritable.get());
	}
}
