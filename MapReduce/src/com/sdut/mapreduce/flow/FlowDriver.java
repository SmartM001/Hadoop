package com.sdut.mapreduce.flow;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {
	public static void main(String[] args)
			throws IOException, InterruptedException, URISyntaxException, ClassNotFoundException {
		// 1 获取配置信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 获取jar的存储路径
		job.setJarByClass(FlowDriver.class);

		// 3 关联map和reduce的class类
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);

		// 4 设置map阶段输出的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// 5 设置最后输出数据的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// // 6 判断输出路径是否存在，如果存在则删除
		// FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),
		// conf, "smart");
		// Path path = new Path(args[1]);
		// if(fs.exists(path)){
		// fs.delete(path, true);
		// }

		// // 设置分区
		// job.setPartitionerClass(FlowPartitioner.class);
		// //设置reducenum
		// job.setNumReduceTasks(5);

		// 7 设置输入数据的路径和输出数据的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 8 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
