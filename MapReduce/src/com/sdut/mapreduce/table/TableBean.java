package com.sdut.mapreduce.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TableBean implements Writable {

	private String order_id; // ����id
	private String p_id; // ��Ʒid
	private int amount; // ��Ʒ����

	private String pname; // ��Ʒ
	private String flag; // ��ı��

	public TableBean() {
		super();
	}

	public TableBean(String order_id, String p_id, int amount, String pname, String flag) {
		super();
		this.order_id = order_id;
		this.p_id = p_id;
		this.amount = amount;
		this.pname = pname;
		this.flag = flag;
	}

	public String getOrder_id() {
		return order_id;
	}

	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}

	public String getP_id() {
		return p_id;
	}

	public void setP_id(String p_id) {
		this.p_id = p_id;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(order_id);
		out.writeUTF(p_id);
		out.writeInt(amount);
		out.writeUTF(pname);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.order_id = in.readUTF();
		this.p_id = in.readUTF();
		this.amount = in.readInt();
		this.pname = in.readUTF();
		this.flag = in.readUTF();
	}

	@Override
	public String toString() {
		return order_id + "\t" + p_id + "\t" + amount + "\t" + pname;
	}

}
