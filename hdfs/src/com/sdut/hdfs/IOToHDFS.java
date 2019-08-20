package com.sdut.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;


public class IOToHDFS {
	
	//文件的上传
	@SuppressWarnings("resource")
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//1获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2获取输出流
		FSDataOutputStream fos = fs.create(new Path("/user/smart/output/zhaoyun.txt"));
		
		//3获取输入流
		FileInputStream fis = new FileInputStream(new File("F:/zhaoyun.txt"));
		
		try {
			//4流对接
			IOUtils.copyBytes(fis, fos, conf);
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			//5关闭资源
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		}
	}
	
	//下载文件
	@SuppressWarnings("resource")
	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//1获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2获取输入流
		FSDataInputStream fis = fs.open(new Path("/user/smart/shuihu.txt"));
		
		//3获取输出流
		FileOutputStream fos = new FileOutputStream(new File("F:/shuihu.txt"));
		
		try {
			//4流的对接
			IOUtils.copyBytes(fis, fos, conf);
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			//5关闭资源
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		}
	}
	
	//定位文件读取,分块读取大文件
	@SuppressWarnings("resource")
	//下载大文件的第一块数据
	@Test
	public void getFileFromHDFSSeek1() throws IOException, InterruptedException, URISyntaxException {
		
		//1获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
				
		//2获取输入流
		FSDataInputStream fis = fs.open(new Path("/user/smart/input/hadoop-2.7.2.tar.gz"));
		
		//3创建输出流
		FileOutputStream fos = new FileOutputStream(new File("F:/hadoop-2.7.2.tar.gz.part1"));
		
		//4流对接（只读取128M）
		byte[] buf = new byte[1024];
		
		for(int i=0;i<1024*128;i++) {
			fis.read(buf);
			fos.write(buf);
		}
		
		try {
			//5关闭资源
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	//下载大文件的第二块
	@SuppressWarnings("resource")
	@Test
	public void getFileFromHDFSSeek2() throws IOException, InterruptedException, URISyntaxException {
		
		//1获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2获取输入流
		FSDataInputStream fis = fs.open(new Path("/user/smart/input/hadoop-2.7.2.tar.gz"));
		
		//3创建输出流
		FileOutputStream fos = new FileOutputStream(new File("F:/hadoop-2.7.2.tar.gz.part2"));
		
		//4流对接(指向第二块数据的首地址)
		//4-0定位到128M
		fis.seek(1024*1024*128);
		
		try {
			IOUtils.copyBytes(fis, fos, conf);
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			//5关闭资源
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		}
	}
}
