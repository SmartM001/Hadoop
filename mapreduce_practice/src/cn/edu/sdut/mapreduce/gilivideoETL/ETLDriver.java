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
		// 1 ��ȡ������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 ����jar·��
		job.setJarByClass(ETLDriver.class);

		// 3 ����map��
		job.setMapperClass(ETLMapper.class);

		// 4 ����map������ݵ�key��value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 5 ��������������ݵ�key��value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 6 ����reduce����
		job.setNumReduceTasks(0);

		// 7 �ж����·���Ƿ���ڣ� ���������ɾ��
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		Path path = new Path(args[1]);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}

		// 8 ��������·�������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 9 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
