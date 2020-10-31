package com.study.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StudentMapper extends Mapper<LongWritable, Text, StudentBean, Text>{

	@Override
	protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, StudentBean, Text>.Context context)
			throws IOException, InterruptedException {

		//切分字段
		String data = v1.toString();
		String[] words = data.split(",");	

		StudentBean sc = new StudentBean();
		sc.setGrades(words[0]);		//给定义的学生类中的班级、成绩赋值（用于排序判断）
		sc.setScore(Double.valueOf(words[2]));

		context.write(sc, v1);

	}
	
}
