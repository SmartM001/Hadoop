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

		// 1 ��ȡjob������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 ���ü���jarλ��
		job.setJarByClass(WordcountDriver.class);

		// 3 ����mapper��reducer��class��
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);

		// 4 �������mapper����������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 ���������������������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// // 6 �ж����·���Ƿ����,���������ɾ��
		// FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),
		// conf, "smart");
		// Path path = new Path(args[1]);
		// if(fs.exists(path)){
		// fs.delete(path, true);
		// }

		// 9 ���÷���
		job.setPartitionerClass(WordcountPartitioner.class);
		// ����reducenum
		job.setNumReduceTasks(2);

		// 10 ����С�ļ�
		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

		// 11 ����Combiner
		job.setCombinerClass(WordcountCombiner.class);

		// 7 �����������ݺ�������ݵ�·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 8 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
