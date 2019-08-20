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
		// 1 ��ȡ������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 ָ���������jar�����ڵı���·��
		job.setJarByClass(TableDriver.class);

		// 3 ָ����ҵ��jobҪʹ�õ�map/reducerҵ����
		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);

		// 4 ָ��map������ݵ�key��value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);

		// 5 ָ������������ݵ�key��value����
		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 6 ָ����������·�����������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 �ύ
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
