package com.sdut.metrix;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 矩阵乘法Driver类
 * @author Ming
 *
 */
public class MetrixDriver {
	
	public static int flag;
	public static int[] shape={};
	
	public static void main(String[] args) throws Exception{
		// 标准化矩阵格式，转化为Map阶段输入格式
		MetrixReader mr = new MetrixReader();
		flag = mr.metrixReader(args[0]);
		shape = mr.metrixShape(args[0]);
		
		// 1、获取配置信息
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		// 2、设置关联jar位置
		job.setJarByClass(MetrixDriver.class);
		
		// 3、设置map和reduce类
		job.setMapperClass(MetrixMapper.class);
		job.setReducerClass(MetrixReducer.class);
		
		// 4、设置map输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// 5、设置最终输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 8 设置输入数据的路径和输出数据的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]+"\\m.csv"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 9 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
