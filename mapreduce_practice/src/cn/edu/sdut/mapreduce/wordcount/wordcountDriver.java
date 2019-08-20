package cn.edu.sdut.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordcountDriver {
	public static void main(String[] args) throws Exception {
		// 1 获取配置信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 设置关联jar位置
		job.setJarByClass(wordcountDriver.class);

		// 3 设置mapper和reducer类
		job.setMapperClass(wordcountMapper.class);
		job.setReducerClass(wordcountReducer.class);

		// 4 设置mapper输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 设置最终输出的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 6 设置分区
		job.setPartitionerClass(wordcountPartitioner.class);
		job.setNumReduceTasks(2);

		// 7 设置Combiner
		job.setCombinerClass(wordcountCombiner.class);

		// 8 设置输入数据的路径和输出数据的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 9 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

}
