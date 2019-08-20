package cn.edu.sdut.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		
		// 1 �Ƚϴ�С
		double maxValue = Double.MIN_VALUE;
		for(DoubleWritable value : values){
			maxValue = Math.max(maxValue, value.get());
		}
		
		// 2 д��
		context.write(key, new DoubleWritable(maxValue));
	}
}
