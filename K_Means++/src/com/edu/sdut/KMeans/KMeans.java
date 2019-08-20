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
 * ����MapReduce��K-Means++�㷨�о���ʵ��
 * 1���ȴ����������ѡ��һ��������Ϊ��������
 * 2���ټ���ÿһ���������þ������ĵľ��룬Ȼ���վ����Ȩѡ�����������ü�Ȩ�����ѡ�񣬴���ʵ�ѡ�����Ƚϴ�����������ڶ�����������
 * 3���ظ��ڶ�����ֱ��ѡ��K����������Ϊֹ
 * 4�����ճ���K-Means�㷨���о���
 * @author Ming
 *
 */

public class KMeans {
	
	static String centerPath = "F:/input/km.csv"; // ��ž������ĵ��ļ�
	static ArrayList<ArrayList<Double>> centers = new ArrayList<ArrayList<Double>>(); // ��������
	static TreeMap<Integer, String> newCenters = new TreeMap<Integer, String>(); // �¾�������
	
	/*
	 * ��������Ȩ��
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
	 * ���ü�Ȩ�����ѡ���ʼ��������
	 */
	public static void WeightRandom(String input, int k) throws IOException{
		// ��ȡ��ʼ�������ģ�ֻ��һ����������
		readCenter();
		
		// ��ʼ�������ĵĸ���
		int k_c = centers.size();
		
		// ��ȡ������Ϣ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		
		// �����ʼ�������ĵĸ����������趨�ľ������ĵĸ���������ÿ��������ÿ���������ĵľ���
		while(k_c < k){
			
			// �������������������������ĵľ���
			ArrayList<WeightCategory> dis = new ArrayList<>();
			
			// ������ۼӺ�
			double sum=0;
			
			// ��ȡ����
			InputStream in = fs.open(new Path(input));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			for(String str="";str!=null;str=br.readLine()){
				System.out.println("��ȡһ������"+str+"�ɹ�");
				try{
					// �з�һ����������
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
			
			// ����Ȩ�������ѡ����һ���������ģ�����ʵ�ѡ�����������
			Random random = new Random();
			int n = random.nextInt((int) sum); // n ��[0,sum)
			for(int i=0;i<dis.size();i++){ // ���̷�ѡ���������
				n -= dis.get(i).getMin();
				if(n<=0){
					String category = dis.get(i).getCategory();// ��ȡѡ��ľ�������
					String[] split = category.split(","); // �з־�������
					ArrayList<Double> l1 = new ArrayList<>();
					for(String s : split){
						l1.add(Double.parseDouble(s));
					}
					centers.add(l1); // ���µľ������ķ���centers
					break;
				}
			}
			
			// ��ʼ�������ĵĸ���
			k_c = centers.size();
		}
		
		
		// ��ѡ��ĳ�ʼ��������д���ļ�
		if(fs.exists(new Path(centerPath))){
			fs.delete(new Path(centerPath), true);
		}
		
		// �ؽ����������ļ�centerPath
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
	 * Map�׶�
	 * ��Ҫ���������ľ���
	 */
	static class KMeans_Mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// ��ȡһ��
			String line = value.toString();
			
			// �з�
			String[] fields = line.split(",");
			
			// �������ĵĸ���
			int k = centers.size();
			
			// ÿ���������������
			int c_id = 0;
			
			// ��С����
			double min = Double.MAX_VALUE;
			
			// ����ÿ��������ÿ���������ĵľ���
			for(int i=0;i<k;i++){
				double distance = 0;
				for(int j=0;j<centers.get(0).size();j++){
					double x = centers.get(i).get(j);
					double y = Double.parseDouble(fields[j]);
					distance += Math.pow((x-y), 2);
				}
				//����������ĳһ��
				c_id = distance < min ? i : c_id;
				// ������С����
				min = distance < min ? distance : min;
			}
			
			// д��ȥ
			context.write(new IntWritable(c_id), value);
		}
	}

	/*
	 * Reduce�׶�
	 * ��Ҫ������¾�������
	 */
	static class KMeans_Reducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		@Override
		protected void reduce(IntWritable key, Iterable<Text> value,Context arg2) 
						throws IOException, InterruptedException {
			
			// �洢�����зֺ������
			ArrayList<ArrayList<Double>> all_data = new ArrayList<>();
			
			// ��������ȡ��������
			Iterator<Text> it = value.iterator();
			
			// �����������ݣ������зִ洢
			while(it.hasNext()){
				String line = it.next().toString(); // ��ȡһ��
				String[] fields = line.split(","); // �з�
				ArrayList<Double> per_data = new ArrayList<>();
				for(int i=0;i<fields.length;i++){
					per_data.add(Double.parseDouble(fields[i]));
				}
				all_data.add(per_data);
			}
			
			// �������ĵ�
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
	 * Driver����
	 * �����������MapReduce
	 */
	public static void Driver(String input, String output, boolean isEnd) throws IOException, ClassNotFoundException, InterruptedException{
		readCenter();
		newCenters = new TreeMap<Integer, String>();
		
		// ��ȡjob������Ϣ
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		// ������������
		job.setJarByClass(KMeans.class);
		
		// ����Map��Reduce��
		job.setMapperClass(KMeans_Mapper.class);
		if(!isEnd){
			job.setReducerClass(KMeans_Reducer.class);
		}
		
		// ���������������
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		// �ж�����ļ��Ƿ����
		FileSystem fs = FileSystem.get(configuration);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
		}
		
		// ��������·�������·��
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		// �ύ
		boolean result = job.waitForCompletion(true);
		System.out.println(result ? 0 : 1);
		
		// ���û�н����������º�ľ�������д���ļ�
		if(!isEnd){
			writeCenter(newCenters);
		}
	}

	/*
	 * ����������д�����
	 */
	private static void writeCenter(TreeMap<Integer, String> map) throws IOException {
		
		// ��ȡ������Ϣ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		
		// ���������ļ�������ڣ�ɾ��
		if(fs.exists(new Path(centerPath))){
			fs.delete(new Path(centerPath), true);
		}
		
		// �ؽ����������ļ�centerPath
		PrintWriter pw = new PrintWriter(fs.create(new Path(centerPath)));
		for(String line : map.values()){
			System.out.println("�¾������ģ�"+line+"д��ɹ�");
			pw.println(line);
		}
		
		pw.close();
	}

	/*
	 * ���������ĴӴ��̶���
	 */
	public static void readCenter() throws IOException {
		
		centers = new ArrayList<ArrayList<Double>>();
		
		// ��ȡ������Ϣ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		
		// ��ȡ��������
		InputStream in = fs.open(new Path(centerPath));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		for(String str="";str!=null;str=br.readLine()){
			System.out.println("��ȡһ����������"+str+"�ɹ�");
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
		System.out.println("�������ĵĸ���"+centers.size());
	}

	/*
	 * �ж����ĵ��Ƿ�������ֵҪ��
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
			System.out.println("��ǰsum:"+sum);
			if(sum<=limit){
				return true;
			}
		}
		else{
			System.out.println("�ȴ���������");
			System.out.println("ԭ���ĵ�ĸ�����"+centers.size()+"�����ĵ������"+newCenters.size());
		}
		
		return false;
	}
	
	/*
	 * ������
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		String output = "F:/output/";
		String input = "F:/input/Wine.csv";
		
		// ѡ���ʼ��������
		WeightRandom(input, 3);
		
		while(true){
			System.out.println("��ʼ����..........");
			if(!isOk(0.0)){
				Driver(input, output, false);
			}
			else{
				Driver(input, output, true);
				break;
			}
		}
		System.out.println("�������..........");
		
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
