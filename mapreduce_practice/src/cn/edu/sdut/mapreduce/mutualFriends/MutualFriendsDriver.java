package cn.edu.sdut.mapreduce.mutualFriends;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFriendsDriver {
	public static void main(String[] args) throws Exception {
		// 1 ��ȡ������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 2 ����jar·��
		job.setJarByClass(MutualFriendsDriver.class);
		
		// 3 ����map��reduce����
		job.setMapperClass(MutualFriendsMapper.class);
		job.setReducerClass(MutualFriendsReducer.class);
		
		// 4 ����map������ݵ�key��value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// 5 ��������������ݵ�key��value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 6 �����������ݺ�������ݵ�·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 7 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : -1);
	}
}
