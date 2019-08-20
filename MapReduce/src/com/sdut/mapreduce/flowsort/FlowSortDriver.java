package com.sdut.mapreduce.flowsort;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowSortDriver {

	public static void main(String[] args)
			throws IOException, InterruptedException, URISyntaxException, ClassNotFoundException {
		// 1 ��ȡ������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 ��ȡjar�Ĵ洢·��
		job.setJarByClass(FlowSortDriver.class);

		// 3 ����map��reduce��class��
		job.setMapperClass(FlowSortMapper.class);
		job.setReducerClass(FlowSortReducer.class);

		// 4 ����map�׶������key��value��
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		// 5 �������������ݵ�key��value��
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// // 6 �ж�output�Ƿ���ڣ�������ɾ��
		// FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),
		// conf, "smart");
		// Path path = new Path(args[1]);
		// if (fs.exists(path)) {
		// fs.delete(path, true);
		// }

		// 7 �����������ݵ�·����������ݵ�·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 8 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
