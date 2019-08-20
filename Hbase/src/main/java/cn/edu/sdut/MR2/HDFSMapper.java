package cn.edu.sdut.MR2;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HDFSMapper extends Mapper<LongWritable, Text, NullWritable, Put>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// 获取一行
		String line = value.toString();
		
		// 切割数据
		String[] fields = line.split("\t");
		
		// 封装put对象
		Put put = new Put(Bytes.toBytes(fields[0]));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fields[2]));
		
		// 写出去
		context.write(NullWritable.get(), put);
	}
}
