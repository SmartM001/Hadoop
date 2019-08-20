package com.sdut.mapreduce.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {

	private String orderId; // �������
	private Double price; // ��Ʒ�۸�

	public OrderBean() {
		super();
	}

	public OrderBean(String orderId, Double price) {
		super();
		this.orderId = orderId;
		this.price = price;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	@Override
	// ���л�
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orderId);
		out.writeDouble(price);
	}

	@Override
	// �����л�
	public void readFields(DataInput in) throws IOException {
		this.orderId = in.readUTF();
		this.price = in.readDouble();
	}

	@Override
	public int compareTo(OrderBean o) {
		// ��������
		// 1 ����id�Ž�������
		int comResult = this.orderId.compareTo(o.getOrderId());

		if (comResult == 0) {
			// 2 ���ռ۸�������
			comResult = this.price > o.getPrice() ? -1 : 1;
		}
		return comResult;
	}

	@Override
	public String toString() {
		return orderId + "\t" + price;
	}

}
