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
		// 1 ��ȡ������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 ���ù���jarλ��
		job.setJarByClass(wordcountDriver.class);

		// 3 ����mapper��reducer��
		job.setMapperClass(wordcountMapper.class);
		job.setReducerClass(wordcountReducer.class);

		// 4 ����mapper�������������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 ���������������������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 6 ���÷���
		job.setPartitionerClass(wordcountPartitioner.class);
		job.setNumReduceTasks(2);

		// 7 ����Combiner
		job.setCombinerClass(wordcountCombiner.class);

		// 8 �����������ݵ�·����������ݵ�·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 9 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

}
