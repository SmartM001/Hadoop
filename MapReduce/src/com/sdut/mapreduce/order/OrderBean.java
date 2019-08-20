package com.sdut.mapreduce.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {

	private String orderId; // 订单编号
	private Double price; // 商品价格

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
	// 序列化
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orderId);
		out.writeDouble(price);
	}

	@Override
	// 反序列化
	public void readFields(DataInput in) throws IOException {
		this.orderId = in.readUTF();
		this.price = in.readDouble();
	}

	@Override
	public int compareTo(OrderBean o) {
		// 两次排序
		// 1 按照id号进行排序
		int comResult = this.orderId.compareTo(o.getOrderId());

		if (comResult == 0) {
			// 2 按照价格倒序排序
			comResult = this.price > o.getPrice() ? -1 : 1;
		}
		return comResult;
	}

	@Override
	public String toString() {
		return orderId + "\t" + price;
	}

}
