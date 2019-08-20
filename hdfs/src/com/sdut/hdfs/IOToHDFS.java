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
	
	//�ļ����ϴ�
	@SuppressWarnings("resource")
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//1��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2��ȡ�����
		FSDataOutputStream fos = fs.create(new Path("/user/smart/output/zhaoyun.txt"));
		
		//3��ȡ������
		FileInputStream fis = new FileInputStream(new File("F:/zhaoyun.txt"));
		
		try {
			//4���Խ�
			IOUtils.copyBytes(fis, fos, conf);
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			//5�ر���Դ
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		}
	}
	
	//�����ļ�
	@SuppressWarnings("resource")
	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//1��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2��ȡ������
		FSDataInputStream fis = fs.open(new Path("/user/smart/shuihu.txt"));
		
		//3��ȡ�����
		FileOutputStream fos = new FileOutputStream(new File("F:/shuihu.txt"));
		
		try {
			//4���ĶԽ�
			IOUtils.copyBytes(fis, fos, conf);
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			//5�ر���Դ
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		}
	}
	
	//��λ�ļ���ȡ,�ֿ��ȡ���ļ�
	@SuppressWarnings("resource")
	//���ش��ļ��ĵ�һ������
	@Test
	public void getFileFromHDFSSeek1() throws IOException, InterruptedException, URISyntaxException {
		
		//1��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
				
		//2��ȡ������
		FSDataInputStream fis = fs.open(new Path("/user/smart/input/hadoop-2.7.2.tar.gz"));
		
		//3���������
		FileOutputStream fos = new FileOutputStream(new File("F:/hadoop-2.7.2.tar.gz.part1"));
		
		//4���Խӣ�ֻ��ȡ128M��
		byte[] buf = new byte[1024];
		
		for(int i=0;i<1024*128;i++) {
			fis.read(buf);
			fos.write(buf);
		}
		
		try {
			//5�ر���Դ
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	//���ش��ļ��ĵڶ���
	@SuppressWarnings("resource")
	@Test
	public void getFileFromHDFSSeek2() throws IOException, InterruptedException, URISyntaxException {
		
		//1��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2��ȡ������
		FSDataInputStream fis = fs.open(new Path("/user/smart/input/hadoop-2.7.2.tar.gz"));
		
		//3���������
		FileOutputStream fos = new FileOutputStream(new File("F:/hadoop-2.7.2.tar.gz.part2"));
		
		//4���Խ�(ָ��ڶ������ݵ��׵�ַ)
		//4-0��λ��128M
		fis.seek(1024*1024*128);
		
		try {
			IOUtils.copyBytes(fis, fos, conf);
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			//5�ر���Դ
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		}
	}
}
