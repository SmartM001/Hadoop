package com.sdut.metrix;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 矩阵乘法Reduce阶段
 * @author Ming
 *
 */
public class MetrixReducer extends Reducer<Text, Text, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		// 1、Map存储数值
		Map<String, String> mA = new HashMap<>();
		Map<String, String> mB = new HashMap<>();
		
		System.out.println(key+":");
		
		// 2、读取每个元素值
		for (Text line : values) {
			String val = line.toString();
			System.out.println(val);
			if(val.startsWith("A")){
				String[] kv = val.substring(2).split(",");
				mA.put(kv[0], kv[1]);
			}
			else if(val.startsWith("B")){
				String[] kv = val.substring(2).split(",");
				mB.put(kv[0], kv[1]);
			}
		}
		
		// 3、计算结果
		int result = 0;
		Iterator<String> iter = mA.keySet().iterator();
		while(iter.hasNext()){
			String mk = iter.next();
			result += Integer.parseInt(mA.get(mk))*Integer.parseInt(mB.get(mk));
		}
		context.write(key, new IntWritable(result));
	}
}
