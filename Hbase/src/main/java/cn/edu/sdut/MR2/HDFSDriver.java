package cn.edu.sdut.MR2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSDriver extends Configuration implements Tool {

	private Configuration configuration;

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return configuration;
	}

	@Override
	public void setConf(Configuration configuration) {
		// TODO Auto-generated method stub
		this.configuration = configuration;
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// 加载job
		Job job = Job.getInstance(configuration);

		// 设置主类
		job.setJarByClass(HDFSDriver.class);

		// 设置Mapper
		job.setMapperClass(HDFSMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Put.class);

		// 设置Reducer
		TableMapReduceUtil.initTableReducerJob("fruit2", HDFSReducer.class, job);

		// 设置输入路径
		FileInputFormat.setInputPaths(job, args[0]);
				
		// 提交
		boolean result = job.waitForCompletion(true);

		return result ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		int i = ToolRunner.run(conf, new HDFSDriver(), args);
		System.exit(i);
	}

}
