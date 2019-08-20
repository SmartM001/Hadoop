package com.sdut.metrix;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;


/**
 * 读取矩阵文件
 * 将矩阵格式标准化，转化为Map阶段的输入格式
 * @author Ming
 *
 */
public class MetrixReader {
	
	/**
	 * 读取矩阵的维度
	 * @param args
	 * @return
	 * @throws Exception 
	 */
	public int[] metrixShape(String args) throws Exception{
		int[] shape = {0,0};
		
		// 1、读文件
		FileReader fr1 = new FileReader(args+"\\shape.csv");
		BufferedReader br1 = new BufferedReader(fr1);
		
		String line = "";
		int count =0;
		
		// 2、读取矩阵的维度
		while((line = br1.readLine())!=null){
			if(count<1){
				shape[0] = Integer.parseInt(line.split(" ")[0]);
			}
			else{
				shape[1] = Integer.parseInt(line.split(" ")[1]);
			}
			count++;
		}
		
		return shape;
	}
	
	/**
	 * 读取矩阵文件
	 * 将矩阵格式标准化，转化为Map阶段的输入格式
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public int metrixReader(String args) throws Exception{
		
		// 1、读文件
		FileReader fr1 = new FileReader(args+"\\m1.csv");
		FileReader fr2 = new FileReader(args+"\\m2.csv");
		BufferedReader br1 = new BufferedReader(fr1);
		BufferedReader br2 = new BufferedReader(fr2);
		
		// 2、写文件
		FileWriter fw = new FileWriter(args+"\\m.csv", true);
		PrintWriter pw = new PrintWriter(fw);
		
		String line = "";
		String result = "";
		int rownum1 = 1;// 矩阵的行数
		int columnnum1=0;// 矩阵的列数
		
		// 3、读取矩阵m1
		while((line = br1.readLine())!=null){
			String[] split = line.split(" ");
			columnnum1 = split.length;
			for(int i=0;i<columnnum1;i++){
				result = rownum1+" "+(i+1)+" "+split[i];
				System.out.println(result);
				pw.println(result);
				pw.flush();
			}
			System.out.println("columnnum:"+columnnum1);
			System.out.println("split的长度："+split.length);
			rownum1++;
		}
		
		int rownum2 = 1;
		int columnnum2=0;
		
		// 4、读取矩阵m2
		while((line = br2.readLine())!=null){
			String[] split = line.split(" ");
			columnnum2 = split.length;
			for(int i=0;i<columnnum2;i++){
				result = rownum2+" "+(i+1)+" "+split[i];
				System.out.println(result);
				pw.println(result);
				pw.flush();
			}
			rownum2++;
		}
		
		// 5、关闭资源
		pw.close();
		fw.close();
		br1.close();
		fr1.close();
		br2.close();
		fr2.close();
		
		System.out.println("rownum1:"+(rownum1-1));
		System.out.println("columnnum:"+columnnum1);
		return ((rownum1-1)*columnnum1);
	}
}
