package cn.edu.sdut.mapreduce.flowsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {
	public static void main(String[] args) throws Exception {
		// 1 ��ȡ������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 2 ����jar��
		job.setJarByClass(FlowDriver.class);
		
		// 3 ����map��reduce��
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		
		// 4 ����map������ݵ�key��value
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		// 5 ��������������ݵ�key��value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		// 6 �����������ݺ�������ݵ�·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 7 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
