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
 * ����˷�Driver��
 * @author Ming
 *
 */
public class MetrixDriver {
	
	public static int flag;
	public static int[] shape={};
	
	public static void main(String[] args) throws Exception{
		// ��׼�������ʽ��ת��ΪMap�׶������ʽ
		MetrixReader mr = new MetrixReader();
		flag = mr.metrixReader(args[0]);
		shape = mr.metrixShape(args[0]);
		
		// 1����ȡ������Ϣ
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		// 2�����ù���jarλ��
		job.setJarByClass(MetrixDriver.class);
		
		// 3������map��reduce��
		job.setMapperClass(MetrixMapper.class);
		job.setReducerClass(MetrixReducer.class);
		
		// 4������map�������������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// 5�����������������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 8 �����������ݵ�·����������ݵ�·��
		FileInputFormat.setInputPaths(job, new Path(args[0]+"\\m.csv"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 9 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
