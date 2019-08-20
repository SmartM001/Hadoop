package com.sdut.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
	@Override
	protected void reduce(OrderBean bean, Iterable<NullWritable> value,
			Context context)throws IOException, InterruptedException {
		// Ð´³ö
		context.write(bean, NullWritable.get());
	}
}
