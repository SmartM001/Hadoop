package cn.edu.sdut.MR1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 从HBase表中读数据，然后写到HBase中的另一个表中
 * 
 * @author Ming
 *
 */
public class FruitDriver extends Configuration implements Tool {

	private Configuration configuration = null;

	@Override
	public int run(String[] arg0) throws Exception {

		// 创建job
		Job job = Job.getInstance(configuration);

		// 指定Driver类
		job.setJarByClass(FruitDriver.class);

		// 指定Mapper
		TableMapReduceUtil.initTableMapperJob("fruit", new Scan(), FruitMapper.class, ImmutableBytesWritable.class,
				Put.class, job);

		// 指定Reducer
		TableMapReduceUtil.initTableReducerJob("fruit_mr", FruitReducer.class, job);

		// 提交
		boolean result = job.waitForCompletion(true);

		return result ? 0 : 1;
	}

	@Override
	public Configuration getConf() {
		return configuration;
	}

	@Override
	public void setConf(Configuration configuration) {
		this.configuration = configuration;
	}

	public static void main(String[] args) throws Exception {
		
		Configuration configuration = HBaseConfiguration.create();
		
		int i = ToolRunner.run(configuration, new FruitDriver(), args);
	}
}
