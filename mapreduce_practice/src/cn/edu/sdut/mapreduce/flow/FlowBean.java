package cn.edu.sdut.mapreduce.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable {

	private long upflow; // ��������
	private long downflow; // ��������
	private long sumflow; // ������

	public FlowBean() {
		super();
	}

	public FlowBean(long upflow, long downflow) {
		super();
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = upflow + downflow;
	}
	
	public void set(long upflow, long downflow) {
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = upflow + downflow;
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

	// ���л�
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upflow);
		out.writeLong(downflow);
		out.writeLong(sumflow);
	}

	// �����л�
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upflow = in.readLong();
		this.downflow = in.readLong();
		this.sumflow = in.readLong();
	}

	@Override
	public String toString() {
		return upflow + "\t" + downflow + "\t" + sumflow;
	}

}
