package com.sdut.mapreduce.wordcount;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordcountDriver {

	public static void main(String[] args) throws Exception {

		// 1 获取job对象信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 设置加载jar位置
		job.setJarByClass(WordcountDriver.class);

		// 3 设置mapper和reducer的class类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);

		// 4 设置输出mapper的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 设置最终数据输出的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// // 6 判断输出路径是否存在,如果存在则删除
		// FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),
		// conf, "smart");
		// Path path = new Path(args[1]);
		// if(fs.exists(path)){
		// fs.delete(path, true);
		// }

		// 9 设置分区
		job.setPartitionerClass(WordcountPartitioner.class);
		// 设置reducenum
		job.setNumReduceTasks(2);

		// 10 处理小文件
		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

		// 11 设置Combiner
		job.setCombinerClass(WordcountCombiner.class);

		// 7 设置输入数据和输出数据的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 8 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
