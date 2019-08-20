package com.sdut.mapreduce.distributedcache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	// 缓存pd.txt数据
	private Map<String, String> pdMap = new HashMap<>();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// 读取pd.txt文件，并把数据存储到缓存
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("F:/input/pd.txt"))));
		
		String line;
		
		while(StringUtils.isNotEmpty(line = reader.readLine())){
			// 截取
			String[] fields = line.split("\t");
			
			// 存储数据到缓存
			pdMap.put(fields[0], fields[1]);			
		}
		
		// 关闭资源
		reader.close();
	}
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 读取一行数据
		String line = value.toString();
		
		// 2 截取
		String[] fields = line.split("\t");
		
		// 3 获取pdname
		String pdname = pdMap.get(fields[1]);
		
		// 4 拼接
		k.set(line + '\t' + pdname);
		
		// 5 写出
		context.write(k, NullWritable.get());
	}
}
