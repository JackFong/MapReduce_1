package com.study.MapReduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StudentBean implements WritableComparable<StudentBean> {
	private String grades;
	private String name;
	private Double score;
	
	public String toString() {
		return grades + "," + name + ","+ score;		//+ name + "\t"
	}  
	public void write(DataOutput output) throws IOException {
		//实现序列化
		output.writeUTF(grades);
		output.writeUTF(name);
		output.writeDouble(score);
		
	}
	
	public void readFields(DataInput input) throws IOException {
		//实现反序列化
		this.grades = input.readUTF();
		this.name = input.readUTF();
		this.score = input.readDouble();
	}
	
	public int compareTo(StudentBean studentBean) {	//指定排序规则
		//先比较班级号（升序），相同则比较成绩（降序）
		int i = this.grades.compareTo(studentBean.grades);
		if(i == 0) {
			i = this.score.compareTo(studentBean.score) * -1;
		
		}
		return i;
	}
	
	
	public String getGrades() {
		return grades;
	}
	public void setGrades(String grades) {
		this.grades = grades;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Double getScore() {
		return score;
	}
	public void setScore(Double score) {
		this.score = score;
	}
}
