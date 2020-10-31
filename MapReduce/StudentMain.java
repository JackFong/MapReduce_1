package com.study.MapReduce;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StudentMain extends Configured implements Tool {		

	public int run(String[] args) throws Exception {		
		//1.获取Job对象,定义Job名称
		Job job = Job.getInstance(super.getConf(), "student_job");
		
			//设置输入类型和输入路径
		job.setInputFormatClass(TextInputFormat.class);		
		TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/input/student.csv"));
		
		//2.设置Job任务	
			//指定map阶段
		job.setMapperClass(StudentMapper.class);
		job.setMapOutputKeyClass(StudentBean.class);	//设置k2类型
		job.setMapOutputValueClass(Text.class);		//设置v2类型
		
		//3.设置shuffle阶段
			//设置分区
		job.setPartitionerClass(StudentPartition.class);
			//设置分组
		job.setGroupingComparatorClass(StudentComparator.class);
		
		//4.指定reducer阶段
		job.setReducerClass(StudentReducer.class);
		job.setOutputKeyClass(Text.class);	//设置k3类型
		job.setOutputValueClass(NullWritable.class);	//设置v3类型
		
			//设置输出类型和输出路径
		job.setOutputFormatClass(TextOutputFormat.class);
		Path path = new Path("hdfs://node1:8020/output/student");
		TextOutputFormat.setOutputPath(job, path);
		//判断输出文件是否存在，存在则删除该文件（避免再次输出时报错）
		FileSystem fs = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());	//构造输入流
		boolean a = fs.exists(path);
		if(a) {
			fs.delete(path, true);
		}
		
		//最后等待job任务结束
		boolean b = job.waitForCompletion(true);
		return b ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		int run = ToolRunner.run(conf, new StudentMain(), args);
		
		System.exit(run);
	}
}
