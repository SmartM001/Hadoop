package cn.edu.sdut.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text, FlowBean>{

	@Override
	public int getPartition(Text k, FlowBean bean, int numPartitioner) {
		// 需求：按照号码归属地进行分区
		
		int partitioner = 7;
		String phoneNum = k.toString().substring(0, 3);
		if("134".equals(phoneNum)){
			partitioner = 0;
		}else if("135".equals(phoneNum)){
			partitioner = 1;
		}else if("136".equals(phoneNum)){
			partitioner = 2;
		}else if("137".equals(phoneNum)){
			partitioner = 3;
		}else if("138".equals(phoneNum)){
			partitioner = 4;
		}else if("139".equals(phoneNum)){
			partitioner = 5;
		}else{
			partitioner = 6;
		}
		
		return partitioner;
	}
}
