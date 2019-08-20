package cn.edu.sdut.MR1;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

public class FruitReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
			throws IOException, InterruptedException {

		// 遍历写出
		for (Put value : values) {
			context.write(NullWritable.get(), value);
		}
	}

}
