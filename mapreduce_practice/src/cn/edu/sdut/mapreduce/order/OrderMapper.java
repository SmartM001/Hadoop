package cn.edu.sdut.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	DoubleWritable d = new DoubleWritable();
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 获取每一行数据
		String line = value.toString();
		
		// 2 截取每个字段
		String[] fields = line.split("\t");
		
		String id = fields[0];
		k.set(id);
		
		double price = Double.parseDouble(fields[2]);
		d.set(price);
		
		// 3 写出
		context.write(k, d);
	}
}
