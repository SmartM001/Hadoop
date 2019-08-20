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
		// 1 ��ȡ������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 ��ȡjar�Ĵ洢·��
		job.setJarByClass(DistributedDriver.class);

		// 3 ����map��reduce��class��
		job.setMapperClass(DistributedMapper.class);

		// 4 �������������ݵ�key��value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 5 ���ػ�������
		job.addCacheFile(new URI("file:/F:/input/pd.txt"));

		// 6 �����������ݵ�·����������ݵ�·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 7 map��join���߼�����Ҫreduce�׶Σ�����reducetask����Ϊ0
		job.setNumReduceTasks(0);

		// 8 �ύ
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
