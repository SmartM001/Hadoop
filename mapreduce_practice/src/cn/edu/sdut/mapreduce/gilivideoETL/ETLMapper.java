package cn.edu.sdut.mapreduce.gilivideoETL;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	private Text text = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// 1 ��ȡ����
		String line = value.toString();
		
		// 2 ��ϴ����
		String etlstr = ETLUtil.etlUtil(line);
		
		// 3 д��
		text.set(etlstr);
		context.write(text, NullWritable.get());
	}
}
