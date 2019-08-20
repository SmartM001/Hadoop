package com.sdut.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text, FlowBean>{

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		
		// 1 需求：根据电话号码的前3位来分区
		
		//拿到电话号码的前三位
		String phoneNum = key.toString().substring(0, 3);
		
		int partitions = 4;
		
		if("135".equals(phoneNum)){
			partitions = 0;
		}
		if("136".equals(phoneNum)){
			partitions = 1;
		}
		if("137".equals(phoneNum)){
			partitions = 2;
		}
		if("138".equals(phoneNum)){
			partitions = 3;
		}
		return partitions;
	}

}
