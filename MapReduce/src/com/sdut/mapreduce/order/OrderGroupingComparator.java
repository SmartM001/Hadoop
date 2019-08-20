package com.sdut.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

	// дһ���ղι���
	public OrderGroupingComparator() {
		super(OrderBean.class, true);
	}

	// ��д�Ƚϵķ���
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;

		// ���ݶ���id�űȽϣ��ж��Ƿ�Ϊһ��
		return aBean.getOrderId().compareTo(bBean.getOrderId());
	}
}
