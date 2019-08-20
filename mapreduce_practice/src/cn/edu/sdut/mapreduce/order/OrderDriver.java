package cn.edu.sdut.mapreduce.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {
		public static void main(String[] args) throws Exception {
			// 1 获取配置信息
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			
			// 2 设置jar类
			job.setJarByClass(OrderDriver.class);
			
			// 3 设置map和reduce类
			job.setMapperClass(OrderMapper.class);
			job.setReducerClass(OrderReducer.class);
			
			// 4 设置map输出数据的key和value
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			
			// 5 设置最终输出数据的key和value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(OrderReducer.class);
			
			// 6 设置输入数据和输出数据的路径
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			// 7 提交
			boolean result = job.waitForCompletion(true);
			System.exit(result ? 0 : 1);
		}
}
