package com.study.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StudentReducer extends Reducer<StudentBean, Text, Text, NullWritable> {

	@Override
	protected void reduce(StudentBean k2, Iterable<Text> v2,
			Reducer<StudentBean, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		System.out.println("Reducer阶段完成1");
		int i =0;
		for(Text v : v2) {
			context.write(v, NullWritable.get());
			i ++;
			
			if(i >= 1) {	//执行一次。只需要成绩最高的学生
				break;
			}
			
		}
		System.out.println("Reducer阶段完成");
	}
	
}
