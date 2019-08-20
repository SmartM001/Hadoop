package cn.edu.sdut.mapreduce.flow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		
		long upFlow = 0;
		long downFlow = 0;
		
		for(FlowBean value : values){
			upFlow = value.getUpflow();
			downFlow = value.getDownflow();
		}
		
		// Êä³ö
		context.write(key, new FlowBean(upFlow, downFlow));
	}
}
