package com.sdut.KMeans;
/**
 * ����MapReduce��KMeans�㷨ʵ��
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
	
	static String centersPath = "F:/input/km.csv";//�������Ĵ洢�ļ���
	static ArrayList<ArrayList<Double>> centers = new ArrayList<ArrayList<Double>>();//ԭ���ĵ�
	static TreeMap<Integer,String> newCenters = new TreeMap<Integer, String>();//�����ĵ�
	
	/*
	 * �ж����ĵ�ı仯�Ƿ�С���趨����ֵ
	 */
	public static boolean isOK(double limit){
		
		// ���ԭ���ĵ�������ĵ㶼������
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
			System.out.println("sum��ǰֵ��" + sum);
			if(sum<=limit){
				return true;
			}
		}
		else{
			System.out.println("ԭ���ĵ�centers:" + centers.size() + "	�����ĵ�newCenters:" + newCenters.size());
		}
		return false;
	}
	
	/*
	 * ����������д������ļ�
	 */
	public static void writeCenters(TreeMap<Integer,String> map) throws IOException{
		// ��ȡ��Ⱥ������Ϣ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		// �жϾ������Ĵ洢�ļ��Ƿ���ڣ�������ڣ�ɾ��
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
	 * ���������Ķ���MapReduce
	 */
	public static void readCenters() throws IOException{
		centers = new ArrayList<ArrayList<Double>>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		// ��ȡ��������
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
	 * Map�׶�
	 * ��Ҫ����������
	 * ����ÿ��������������ĵ�ľ��룬�����ݾ��ൽĳһ��
	 */
 	static class KMeans_Mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// ��ȡһ������
			String line = value.toString();
			String[] fields = line.split(",");
			
			// ������������
			int k = centers.size();
			
			// ÿ�����ݵ��������map�׶ε����key��
			int c_id = 0;
			
			// ÿ�����������ĵ����С����
			Double min = Double.MAX_VALUE;
			
			// ����ÿ��������ÿ�����ĵ�ľ���
			for(int i=0; i<k; i++){ 
				double sum = 0;
				for(int j=0; j<centers.get(0).size(); j++){
					double x = centers.get(i).get(j); // ���ĵ�����
					double y = Double.parseDouble(fields[j]); // һ����������
					sum += Math.pow((x-y), 2); // �����������������ĵ�ľ���
				}
				// ���������ݾ��ൽĳһ��
				c_id = min < sum ? c_id : i+1;
				// ������С����
				min = min < sum ? min : sum;
			}
			
			context.write(new IntWritable(c_id), value);
		}
	}
	
	/*
	 * Reduce�׶�
	 * ����map�׶ξ���Ľ��������ÿ������ľ�������
	 */
	static class KMeans_Reducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			// ��������
			ArrayList<ArrayList<Double>> all_data = new ArrayList<ArrayList<Double>>(); 
			
			// ����������ȡ��������
			Iterator<Text> it = values.iterator();
			while(it.hasNext()){
				String line = it.next().toString(); // ��ȡһ��
				String[] fields = line.split(","); // �з�
				ArrayList<Double> per_data = new ArrayList<Double>();
				for(int i=0; i<fields.length; i++){
					per_data.add(Double.parseDouble(fields[i]));
				}
				all_data.add(per_data);
			}
			
			// �������ĵ�
			String result = "";//���ĵ�
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
			
			// �������ĵ�
			newCenters.put(key.get(), result);
		}
	}

	/*
	 * Dirver
	 * �����������MapReduce
	 */
	public static void Driver(String input, String output, boolean isEnd) throws IOException, ClassNotFoundException, InterruptedException{
		
		// ��ȡ���ĵ�
		readCenters();
		
		newCenters = new TreeMap<Integer, String>();
		
		// 1 ��ȡjob������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		// 2 ���ü���jarλ��
		job.setJarByClass(KMeans.class);
		
		// 3 ����mapper��reducer��class��
		job.setMapperClass(KMeans_Mapper.class);
		if(!isEnd)
		{
			job.setReducerClass(KMeans_Reducer.class);
		}
		
		// 4 ���������������
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		// 5 �ж�����ļ��Ƿ����
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output),true);
		}
		
		// 6 �����������ݺ�������ݵ�·��
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		// 7 �ύ
		boolean result= job.waitForCompletion(true);
		System.out.println(result?0:1);
		
		// 8 ���û�н������������ĵ�д���ļ�
		if(!isEnd){
			writeCenters(newCenters);
		}
	}
	
	/*
	 * �����򷽷�
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
		
		System.out.println("ִ�����");
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
