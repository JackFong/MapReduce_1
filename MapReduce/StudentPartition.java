package com.study.MapReduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StudentPartition extends Partitioner<StudentBean, Text>{
	
	/*
	 * k2: stuc
	 * v2: text
	 */
	@Override
	public int getPartition(StudentBean stuBean, Text text, int numPartitions) {
		System.out.println("分区阶段");
		return (stuBean.getGrades().hashCode() & 2147483647) % numPartitions;		//以班级号定义分区规则
	
	}
	
}
