package com.sdut.mapreduce.table;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean>{
	
	TableBean bean = new TableBean();
	Text k = new Text();
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException ,InterruptedException {
		// 1 ��ȡ�ļ�����
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		String name = inputSplit.getPath().getName();
		
		// 2 ��ȡһ������
		String line = value.toString();
		
		// 3 ��ͬ�ļ����в�ͬ�Ĳ�������װbean����
		if(name.startsWith("order")){ // ���������Ϣ����
			String[] fields = line.split("\t");
			
			bean.setOrder_id(fields[0]);
			bean.setP_id(fields[1]);
			bean.setAmount(Integer.parseInt(fields[2]));
			
			bean.setPname("");
			bean.setFlag("0");
			
			// ����keyֵ
			k.set(fields[1]);
			
		}else{ // ��Ʒ�����Ϣ����
			String[] fields = line.split("\t");
			
			bean.setOrder_id("");
			bean.setP_id(fields[0]);
			bean.setAmount(0);
			
			bean.setPname(fields[1]);
			bean.setFlag("1");
			
			// ����keyֵ
			k.set(fields[0]);
		}
		
		// 4 д��
		context.write(k, bean);
	}
}
