package com.sdut.mapreduce.wholefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class WholeDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WholeDriver.class);

		// �����Զ����inputformat
		job.setInputFormatClass(WholeFileInputformat.class);
		// ��������ļ��ĸ�ʽΪsequencefile
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(WholeMapper.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
