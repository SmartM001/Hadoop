package com.sdut.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

	// 写一个空参构造
	public OrderGroupingComparator() {
		super(OrderBean.class, true);
	}

	// 重写比较的方法
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;

		// 根据订单id号比较，判断是否为一组
		return aBean.getOrderId().compareTo(bBean.getOrderId());
	}
}
