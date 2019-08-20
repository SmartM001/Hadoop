package cn.edu.sdut.mapreduce.gilivideoETL;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ETLDriver {
	public static void main(String[] args) throws Exception {
		// 1 获取配置信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 设置jar路径
		job.setJarByClass(ETLDriver.class);

		// 3 关联map类
		job.setMapperClass(ETLMapper.class);

		// 4 设置map输出数据的key和value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 5 设置最终输出数据的key和value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 6 设置reduce数量
		job.setNumReduceTasks(0);

		// 7 判断输出路径是否存在， 如果存在则删除
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		Path path = new Path(args[1]);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}

		// 8 设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 9 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
