package com.sdut.mapreduce.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TableDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1 获取配置信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 指定本程序的jar包所在的本地路径
		job.setJarByClass(TableDriver.class);

		// 3 指定本业务job要使用的map/reducer业务类
		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);

		// 4 指定map输出数据的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);

		// 5 指定最终输出数据的key和value类型
		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 6 指定输入数据路径和输出数据路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 提交
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
