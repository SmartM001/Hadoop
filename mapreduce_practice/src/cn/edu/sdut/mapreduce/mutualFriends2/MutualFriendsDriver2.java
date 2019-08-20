package cn.edu.sdut.mapreduce.mutualFriends2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFriendsDriver2 {
	public static void main(String[] args) throws Exception {
		// 1 获取配置信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 2 设置jar类路径
		job.setJarByClass(MutualFriendsDriver2.class);
		
		// 3 设置关联map和reduce的类
		job.setMapperClass(MutualFriendsMapper2.class);
		job.setReducerClass(MutualFriendsReducer2.class);
		
		// 4 设置mapper输出数据的key和value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// 5 设置最终输出数据的key和value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 6 设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 7 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : -1);
	}
}
