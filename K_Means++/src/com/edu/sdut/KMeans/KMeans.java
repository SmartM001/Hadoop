package com.edu.sdut.KMeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
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

/**
 * 基于MapReduce的K-Means++算法研究与实现
 * 1、先从样本中随机选择一个样本作为聚类中心
 * 2、再计算每一个样本到该聚类中心的距离，然后按照距离加权选择样本（采用加权随机法选择，大概率的选择距离比较大的样本）做第二个聚类中心
 * 3、重复第二步，直到选出K个聚类中心为止
 * 4、按照常规K-Means算法进行聚类
 * @author Ming
 *
 */

public class KMeans {
	
	static String centerPath = "F:/input/km.csv"; // 存放聚类中心的文件
	static ArrayList<ArrayList<Double>> centers = new ArrayList<ArrayList<Double>>(); // 聚类中心
	static TreeMap<Integer, String> newCenters = new TreeMap<Integer, String>(); // 新聚类中心
	
	/*
	 * 样本距离权重
	 */
	static class WeightCategory {  
        private String category;  
        private Double min;  
          
      
        public WeightCategory() {  
            super();  
        }  
      
        public WeightCategory(String category, Double min) {  
            super();  
            this.setCategory(category);  
            this.setMin(min);  
        }  
      
      
        public Double getMin() {  
            return min;  
        }  
      
        public void setMin(Double min) {  
            this.min = min;  
        }  
      
        public String getCategory() {  
            return category;  
        }  
      
        public void setCategory(String category) {  
            this.category = category;  
        }  
    }
	
	/*
	 * 采用加权随机法选择初始聚类中心
	 */
	public static void WeightRandom(String input, int k) throws IOException{
		// 读取初始聚类中心，只有一个聚类中心
		readCenter();
		
		// 初始聚类中心的个数
		int k_c = centers.size();
		
		// 获取配置信息
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		
		// 如果初始聚类中心的个数不满足设定的聚类中心的个数，计算每条样本与每个聚类中心的距离
		while(k_c < k){
			
			// 存放样本、样本与最近聚类中心的距离
			ArrayList<WeightCategory> dis = new ArrayList<>();
			
			// 距离的累加和
			double sum=0;
			
			// 读取样本
			InputStream in = fs.open(new Path(input));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			for(String str="";str!=null;str=br.readLine()){
				System.out.println("读取一条样本"+str+"成功");
				try{
					// 切分一条样本数据
					String[] fields = str.split(",");
					
					
					double min = Double.MAX_VALUE;
					
					for(int i=0;i<k_c;i++){
						double distance = 0;
						for(int j=0;j<centers.get(0).size();j++){
							double x = centers.get(i).get(j);
							double y = Double.parseDouble(fields[j]);
							distance += Math.pow((x-y), 2);
						}
						min = distance < min ? distance : min;
					}
					sum += min;
					dis.add(new WeightCategory(str, min));
					
				}catch(Exception e){}
			}
			br.close();
			
			// 采用权重随机法选择下一个聚类中心，大概率的选择距离大的样本
			Random random = new Random();
			int n = random.nextInt((int) sum); // n 在[0,sum)
			for(int i=0;i<dis.size();i++){ // 轮盘法选择聚类中心
				n -= dis.get(i).getMin();
				if(n<=0){
					String category = dis.get(i).getCategory();// 获取选择的聚类中心
					String[] split = category.split(","); // 切分聚类中心
					ArrayList<Double> l1 = new ArrayList<>();
					for(String s : split){
						l1.add(Double.parseDouble(s));
					}
					centers.add(l1); // 将新的聚类中心放入centers
					break;
				}
			}
			
			// 初始聚类中心的个数
			k_c = centers.size();
		}
		
		
		// 将选择的初始聚类中心写入文件
		if(fs.exists(new Path(centerPath))){
			fs.delete(new Path(centerPath), true);
		}
		
		// 重建聚类中心文件centerPath
		String result="";
		PrintWriter pw = new PrintWriter(fs.create(new Path(centerPath)));
		for(int i=0;i<k;i++){
			for(int j=0;j<centers.get(0).size();j++){
				if(j==0){
					result += centers.get(i).get(j);
				}
				else{
					result += (","+centers.get(i).get(j));
				}
			}
			pw.println(result);
		}
		pw.close();
	}
	
	/*
	 * Map阶段
	 * 主要负责样本的聚类
	 */
	static class KMeans_Mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// 读取一行
			String line = value.toString();
			
			// 切分
			String[] fields = line.split(",");
			
			// 聚类中心的个数
			int k = centers.size();
			
			// 每条样本的所属类别
			int c_id = 0;
			
			// 最小距离
			double min = Double.MAX_VALUE;
			
			// 计算每条样本与每个聚类中心的距离
			for(int i=0;i<k;i++){
				double distance = 0;
				for(int j=0;j<centers.get(0).size();j++){
					double x = centers.get(i).get(j);
					double y = Double.parseDouble(fields[j]);
					distance += Math.pow((x-y), 2);
				}
				//样本归属于某一类
				c_id = distance < min ? i : c_id;
				// 更新最小距离
				min = distance < min ? distance : min;
			}
			
			// 写出去
			context.write(new IntWritable(c_id), value);
		}
	}

	/*
	 * Reduce阶段
	 * 主要负责更新聚类中心
	 */
	static class KMeans_Reducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		@Override
		protected void reduce(IntWritable key, Iterable<Text> value,Context arg2) 
						throws IOException, InterruptedException {
			
			// 存储所有切分后的数据
			ArrayList<ArrayList<Double>> all_data = new ArrayList<>();
			
			// 迭代器读取所有数据
			Iterator<Text> it = value.iterator();
			
			// 遍历所有数据，进行切分存储
			while(it.hasNext()){
				String line = it.next().toString(); // 读取一行
				String[] fields = line.split(","); // 切分
				ArrayList<Double> per_data = new ArrayList<>();
				for(int i=0;i<fields.length;i++){
					per_data.add(Double.parseDouble(fields[i]));
				}
				all_data.add(per_data);
			}
			
			// 更新中心点
			String result = "";
			for(int i=0;i<all_data.get(0).size();i++){
				double sum=0;
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
			
			newCenters.put(key.get(), result);
		}
	}
	
	/*
	 * Driver方法
	 * 负责调度整个MapReduce
	 */
	public static void Driver(String input, String output, boolean isEnd) throws IOException, ClassNotFoundException, InterruptedException{
		readCenter();
		newCenters = new TreeMap<Integer, String>();
		
		// 获取job对象信息
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		// 设置主程序类
		job.setJarByClass(KMeans.class);
		
		// 设置Map和Reduce类
		job.setMapperClass(KMeans_Mapper.class);
		if(!isEnd){
			job.setReducerClass(KMeans_Reducer.class);
		}
		
		// 设置最终输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		// 判断输出文件是否存在
		FileSystem fs = FileSystem.get(configuration);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
		}
		
		// 设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		// 提交
		boolean result = job.waitForCompletion(true);
		System.out.println(result ? 0 : 1);
		
		// 如果没有结束，将更新后的聚类中心写入文件
		if(!isEnd){
			writeCenter(newCenters);
		}
	}

	/*
	 * 将聚类中心写入磁盘
	 */
	private static void writeCenter(TreeMap<Integer, String> map) throws IOException {
		
		// 获取配置信息
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		
		// 聚类中心文件如果存在，删除
		if(fs.exists(new Path(centerPath))){
			fs.delete(new Path(centerPath), true);
		}
		
		// 重建聚类中心文件centerPath
		PrintWriter pw = new PrintWriter(fs.create(new Path(centerPath)));
		for(String line : map.values()){
			System.out.println("新聚类中心："+line+"写入成功");
			pw.println(line);
		}
		
		pw.close();
	}

	/*
	 * 将聚类中心从磁盘读出
	 */
	public static void readCenter() throws IOException {
		
		centers = new ArrayList<ArrayList<Double>>();
		
		// 获取配置信息
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		
		// 读取聚类中心
		InputStream in = fs.open(new Path(centerPath));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		for(String str="";str!=null;str=br.readLine()){
			System.out.println("读取一条聚类中心"+str+"成功");
			try{
				String[] line = str.split(",");
				ArrayList<Double> l1 = new ArrayList<>();
				for(String s : line){
					l1.add(Double.parseDouble(s));
				}
				centers.add(l1);
			}catch(Exception e){}
		}
		br.close();
		System.out.println("聚类中心的个数"+centers.size());
	}

	/*
	 * 判断中心点是否满足阈值要求
	 */
	public static boolean isOk(double limit){
		
		if(centers.size()>0 && newCenters.size()>0){
			ArrayList<String> nc = new ArrayList<String>(newCenters.values());
			double sum = 0;
			for(int i=0;i<nc.size();i++){
				String line = nc.get(i).toString();
				String[] fields = line.split(",");
				for(int j=0;j<fields.length;j++){
					double x = centers.get(i).get(j);
					double y = Double.parseDouble(fields[j]);
					sum = Math.pow((x-y), 2);
				}
			}
			sum = Math.sqrt(sum);
			System.out.println("当前sum:"+sum);
			if(sum<=limit){
				return true;
			}
		}
		else{
			System.out.println("等待输入数据");
			System.out.println("原中心点的个数："+centers.size()+"新中心点个数："+newCenters.size());
		}
		
		return false;
	}
	
	/*
	 * 主程序
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		String output = "F:/output/";
		String input = "F:/input/Wine.csv";
		
		// 选择初始聚类中心
		WeightRandom(input, 3);
		
		while(true){
			System.out.println("开始聚类..........");
			if(!isOk(0.0)){
				Driver(input, output, false);
			}
			else{
				Driver(input, output, true);
				break;
			}
		}
		System.out.println("聚类结束..........");
		
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
