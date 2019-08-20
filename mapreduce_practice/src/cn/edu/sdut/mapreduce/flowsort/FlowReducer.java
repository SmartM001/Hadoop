package cn.edu.sdut.mapreduce.flowsort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
	
	@Override
	protected void reduce(FlowBean bean, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			
		Text v = values.iterator().next();
		
		context.write(v, bean);
	}
}
