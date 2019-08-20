package com.sdut.KMeans;
/**
 * 基于MapReduce的KMeans算法实现
 * @author Ming
 *
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {
	
	static String centersPath = "F:/input/km.csv";//聚类中心存储文件名
	static ArrayList<ArrayList<Double>> centers = new ArrayList<ArrayList<Double>>();//原中心点
	static TreeMap<Integer,String> newCenters = new TreeMap<Integer, String>();//新中心点
	
	/*
	 * 判断中心点的变化是否小于设定的阈值
	 */
	public static boolean isOK(double limit){
		
		// 如果原中心点和新中心点都有数据
		if(centers.size()>0 && newCenters.size()>0){
			ArrayList<String> nc = new ArrayList<String>(newCenters.values());
			double sum = 0;
			for(int i=0;i<nc.size();i++){
				String line = nc.get(i).toString();
				String[] fields = line.split(",");
				for(int j=0;j<fields.length;j++){
					double x = centers.get(i).get(j);
					double y = Double.parseDouble(fields[j]);
					sum += Math.pow(x-y, 2);
				}
			}
			sum = Math.sqrt(sum);
			System.out.println("sum当前值：" + sum);
			if(sum<=limit){
				return true;
			}
		}
		else{
			System.out.println("原中心点centers:" + centers.size() + "	新中心点newCenters:" + newCenters.size());
		}
		return false;
	}
	
	/*
	 * 将聚类中心写入磁盘文件
	 */
	public static void writeCenters(TreeMap<Integer,String> map) throws IOException{
		// 获取集群配置信息
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		// 判断聚类中心存储文件是否存在，如果存在，删除
		if(fs.exists(new Path(centersPath))){
			fs.delete(new Path(centersPath), true);
		}
		
		PrintWriter pw = new PrintWriter(fs.create(new Path(centersPath)));
		for(String line : map.values()){
			System.out.println("write:" + line);
			pw.println(line);
		}
		pw.close();
	}

	/*
	 * 将聚类中心读入MapReduce
	 */
	public static void readCenters() throws IOException{
		centers = new ArrayList<ArrayList<Double>>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		// 读取聚类中心
		InputStream in = fs.open(new Path(centersPath));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		for(String str="";str!=null;str=br.readLine()){
			System.out.println("read:" + str);
			try{
				String[] line = str.split(",");
				ArrayList<Double> t1 = new ArrayList<Double>();
				for(String s : line){
					t1.add(Double.parseDouble(s));
				}
				centers.add(t1);
			}catch(Exception e){}
		}
		br.close();
		System.out.println("centersSize" + centers.size());
	}
	
	/*
	 * Map阶段
	 * 主要负责聚类操作
	 * 计算每条数据与聚类中心点的距离，将数据聚类到某一类
	 */
 	static class KMeans_Mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 读取一行数据
			String line = value.toString();
			String[] fields = line.split(",");
			
			// 聚类的类别数量
			int k = centers.size();
			
			// 每个数据的所属类别（map阶段的输出key）
			int c_id = 0;
			
			// 每条数据与中心点的最小距离
			Double min = Double.MAX_VALUE;
			
			// 计算每条数据与每个中心点的距离
			for(int i=0; i<k; i++){ 
				double sum = 0;
				for(int j=0; j<centers.get(0).size(); j++){
					double x = centers.get(i).get(j); // 中心点向量
					double y = Double.parseDouble(fields[j]); // 一条样本向量
					sum += Math.pow((x-y), 2); // 计算样本数据与中心点的距离
				}
				// 将样本数据聚类到某一类
				c_id = min < sum ? c_id : i+1;
				// 更新最小距离
				min = min < sum ? min : sum;
			}
			
			context.write(new IntWritable(c_id), value);
		}
	}
	
	/*
	 * Reduce阶段
	 * 根据map阶段聚类的结果，更新每个聚类的聚类中心
	 */
	static class KMeans_Reducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			// 所有数据
			ArrayList<ArrayList<Double>> all_data = new ArrayList<ArrayList<Double>>(); 
			
			// 迭代器，获取所有数据
			Iterator<Text> it = values.iterator();
			while(it.hasNext()){
				String line = it.next().toString(); // 读取一行
				String[] fields = line.split(","); // 切分
				ArrayList<Double> per_data = new ArrayList<Double>();
				for(int i=0; i<fields.length; i++){
					per_data.add(Double.parseDouble(fields[i]));
				}
				all_data.add(per_data);
			}
			
			// 计算中心点
			String result = "";//中心点
			for(int i=0;i<all_data.get(0).size();i++){
				double sum = 0;
				for(int j=0;j<all_data.size();j++){
					sum += all_data.get(j).get(i);
				}
				if(i==0){
					result += (sum/all_data.size());
				}
				else{
					result += ("," + (sum/all_data.size()));
				}
			}
			
			// 更新中心点
			newCenters.put(key.get(), result);
		}
	}

	/*
	 * Dirver
	 * 负责调度整个MapReduce
	 */
	public static void Driver(String input, String output, boolean isEnd) throws IOException, ClassNotFoundException, InterruptedException{
		
		// 读取中心点
		readCenters();
		
		newCenters = new TreeMap<Integer, String>();
		
		// 1 获取job对象信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 2 设置加载jar位置
		job.setJarByClass(KMeans.class);
		
		// 3 设置mapper和reducer的class类
		job.setMapperClass(KMeans_Mapper.class);
		if(!isEnd)
		{
			job.setReducerClass(KMeans_Reducer.class);
		}
		
		// 4 设置最终输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		// 5 判断输出文件是否存在
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output),true);
		}
		
		// 6 设置输入数据和输出数据的路径
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		// 7 提交
		boolean result= job.waitForCompletion(true);
		System.out.println(result?0:1);
		
		// 8 如果没有结束，将新中心点写入文件
		if(!isEnd){
			writeCenters(newCenters);
		}
	}
	
	/*
	 * 主程序方法
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		
		String output = "F:/output/";
		String input = "F:/input/Wine.csv";
		
		while(true){
			System.out.println("start...");
			if(!isOK(0.0)){
				Driver(input, output, false);
			}
			else{
				Driver(input, output, true);
				break;
			}
			System.out.println("end");
		}
		
		System.out.println("执行完成");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] files = fs.listStatus(new Path(output));
		for(FileStatus file : files){
			Path path = file.getPath();
			System.out.println(path);
			InputStream in = fs.open(path);
		}
	}
}
