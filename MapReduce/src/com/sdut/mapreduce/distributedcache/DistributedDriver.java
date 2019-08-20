package com.sdut.mapreduce.distributedcache;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistributedDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		// 1 获取配置信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 获取jar的存储路径
		job.setJarByClass(DistributedDriver.class);

		// 3 关联map和reduce的class类
		job.setMapperClass(DistributedMapper.class);

		// 4 设置最后输出数据的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 5 加载缓存数据
		job.addCacheFile(new URI("file:/F:/input/pd.txt"));

		// 6 设置输入数据的路径和输出数据的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 7 map端join的逻辑不需要reduce阶段，设置reducetask数量为0
		job.setNumReduceTasks(0);

		// 8 提交
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
