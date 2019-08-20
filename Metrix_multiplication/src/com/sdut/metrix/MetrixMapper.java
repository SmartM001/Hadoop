package com.sdut.metrix;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 矩阵乘法Map阶段
 * 4*3矩阵与3*2矩阵的乘法
 * @author Ming
 *
 */
public class MetrixMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private MetrixDriver md = new MetrixDriver();
	private int flag = 0; //区分m1,m2
	private int flag1 = md.flag;
	private int[] shape = md.shape;
	private int[] count1=new int[shape[1]];//结果的列数
	private int[] count2=new int[shape[0]];//结果的行数
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		flag++;
		
		for(int i=0; i<shape[0]; i++){// 结果的行数
			count2[i] = i+1;
		}
		
		for(int i=0; i<shape[1]; i++){// 结果的列数
			count1[i] = i+1;
		}
		
		// 1、读取一行
		String line = value.toString();
		
		// 2、切分数据
		String[] split = line.split(" ");
		
		// 3、如果是矩阵1
		if(flag<=flag1){
			for (int i=0; i<count1.length; i++) {
				String k = split[0]+','+count1[i];
				String v = "A,"+split[1]+','+split[2];
				System.out.println(k+":"+v);
				context.write(new Text(k), new Text(v));
			}
		}
		
		// 4、如果是矩阵2
		else{
			for (int i=0; i<count2.length; i++) {
				String k = ""+count2[i]+","+split[1];
				String v = "B,"+split[0]+","+split[2];
				System.out.println(k+":"+v);
				context.write(new Text(k), new Text(v));
			}
		}
	}
}
