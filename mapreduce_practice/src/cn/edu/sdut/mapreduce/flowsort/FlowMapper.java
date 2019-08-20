package cn.edu.sdut.mapreduce.flowsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	
	FlowBean bean = new FlowBean();
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 ��ȡһ������
		String line = value.toString();
		
		// 2 �з�
		String[] fields = line.split("\t");
		
		// 3 ���л�
		long upflow = Long.parseLong(fields[1]);
		long downflow = Long.parseLong(fields[2]);
		long sumflow = Long.parseLong(fields[3]);
		
		bean.set(upflow, downflow, sumflow);
		k.set(fields[0]);
		
		// 4 д��
		context.write(bean, k);
	}
}
