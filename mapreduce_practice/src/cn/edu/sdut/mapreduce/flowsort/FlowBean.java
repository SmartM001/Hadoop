package cn.edu.sdut.mapreduce.flowsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
	
	private long upflow; // 上行流量
	private long downflow; // 下行流量
	private long sumflow; // 总流量
	
	public FlowBean() {
		super();
	}
	
	public FlowBean(long upflow, long downflow, long sumflow) {
		super();
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = sumflow;
	}
	
	public void set(long upflow, long downflow, long sumflow) {
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = sumflow;
	}
	
	public long getUpflow() {
		return upflow;
	}

	public void setUpflow(long upflow) {
		this.upflow = upflow;
	}

	public long getDownflow() {
		return downflow;
	}

	public void setDownflow(long downflow) {
		this.downflow = downflow;
	}

	public long getSumflow() {
		return sumflow;
	}

	public void setSumflow(long sumflow) {
		this.sumflow = sumflow;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upflow);
		out.writeLong(downflow);
		out.writeLong(sumflow);		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upflow = in.readLong();
		this.downflow = in.readLong();
		this.sumflow = in.readLong();
	}

	@Override
	public int compareTo(FlowBean o) {
		
		return this.sumflow > o.getSumflow() ? -1 : 1;
	}

	@Override
	public String toString() {
		return upflow + "\t" + downflow + "\t" + sumflow;
	}
	
}
