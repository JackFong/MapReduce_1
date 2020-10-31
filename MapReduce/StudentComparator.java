package com.study.MapReduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StudentComparator extends WritableComparator {
	public StudentComparator() {	//调用父类有参构造
		super(StudentBean.class, true);
	}
	
	//重写方法，指定分组的规则；对Map<key，value>中的key进行分组
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		//1.对形参作强制类型转换
		StudentBean first = (StudentBean)a;
		StudentBean second = (StudentBean)b;
		
		System.out.println("分组阶段");
		//2.指定分组规则
		return first.getGrades().compareTo(second.getGrades());	//对两个班级号进行判断，相同则为同一组
	}
	
	
	
}
