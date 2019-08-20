package com.sdut.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HDFSClient {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		
		//0��ȡ������Ϣ
		Configuration conf = new Configuration();
		
		//1��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2�����ļ����ݵ���Ⱥ
		fs.copyFromLocalFile(new Path("F:/xiyou.txt"), new Path("/user/smart/xiyou.txt"));
	}
	
	//��ȡ�ļ�ϵͳ
	@Test
	public void getFileSystem() throws IOException, InterruptedException, URISyntaxException {
		
		//0��ȡ������Ϣ
		Configuration conf = new Configuration();
		
		//1��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2��ӡ�ļ�ϵͳ
		System.out.println(fs.toString());
		
		//3�ر���Դ
		fs.close();
	}
	
	//�ϴ��ļ�
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0��ȡ������Ϣ
		Configuration conf = new Configuration();
		
		//1��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2ִ���ϴ��ļ�����,true��ʾ�Ƿ�ɾ��Դ�ļ�
		fs.copyFromLocalFile(true, new Path("F:/shuihu.txt"), new Path("/user/smart"));
		
		//3�ر���Դ
		fs.close();
	}
	
	//�����ļ�
	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0��ȡ������Ϣ
		Configuration conf = new Configuration();
		
		//1��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2ִ�������ļ�����,��һ�ַ�ʽ����ʹ�������������ļ��ǿ��ļ������ԣ�ʹ�õڶ��ִ�У��͵ķ�ʽ
		//fs.copyToLocalFile(new Path("/user/smart/xiyouji.txt"), new Path("F:/xiyouji.txt"));
		fs.copyToLocalFile(false, new Path("/user/smart/xiyouji.txt"), new Path("F:/xiyouji.txt"), true);
		
		//3�ر���Դ
		fs.close();
	}
	
	//����Ŀ¼
	@Test
	public void mkdirAtHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0��ȡ������Ϣ
		Configuration conf = new Configuration();
		
		//1��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2ִ�д���Ŀ¼����,�ɴ����༶Ŀ¼
		//fs.mkdirs(new Path("/user/smart/output"));
		fs.mkdirs(new Path("/user/smart/xiyouji/houzi/sunwukong"));
		
		//3�ر���Դ
		fs.close();
		
	}
	
	//ɾ���ļ�
	@Test
	public void deleteAtHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0��ȡ������Ϣ
		Configuration conf = new Configuration();
		
		//1��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2ִ��ɾ���ļ��Ĳ���
		fs.delete(new Path("/user/smart/xiyouji.txt"),true);
		
		//3�ر���Դ
		fs.close();
	}
	
	//�������ļ�
	@Test
	public void renameAtHDFS() throws IOException, InterruptedException, URISyntaxException{
		
		//0��ȡ������Ϣ
		Configuration conf = new Configuration();
		
		//1��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2ִ���������ļ��Ĳ���
		fs.rename(new Path("/user/smart/xiyouji"), new Path("/user/smart/xiyou"));
		
		//3�ر���Դ
		fs.close();
	}
	
	//�鿴�ļ�����
	@Test
	public void readFileAtHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0��ȡ������Ϣ
		Configuration conf = new Configuration();
		
		//1��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2ִ�в鿴�ļ��������
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		while(listFiles.hasNext()) {
			
			LocatedFileStatus status = listFiles.next();
			
			// �ļ�����
			System.out.println(status.getPath().getName());
			
			// ���С
			System.out.println(status.getBlockSize());
			
			// �ļ����ݵĳ���
			System.out.println(status.getLen());
			
			// �ļ�Ȩ��
			System.out.println(status.getPermission());
			
			System.out.println("-------------------");
			
			// �ļ���ľ�����Ϣ
			BlockLocation[] blockLocations = status.getBlockLocations();
			
			for(BlockLocation block : blockLocations) {
				System.out.println(block.getOffset());
				
				String[] hosts = block.getHosts();
				
				for(String host:hosts) {
					System.out.println(host);
				}
			}
		}
			
		//3�ر���Դ
		fs.close();
	}
	
	// ��ȡ�ļ��к��ļ���Ϣ
	@Test
	public void readFloderAtHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0��ȡ������Ϣ
		Configuration conf = new Configuration();
		
		//1��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2ִ�л�ȡ�ļ��к��ļ���Ϣ���ж����ļ��л����ļ�
		FileStatus[] listStatus = fs.listStatus(new Path("/user/smart/"));
		
		for(FileStatus status : listStatus) {
			
			if(status.isFile()) {
				System.out.println("f---" + status.getPath().getName());
			}else {
				System.out.println("d---" + status.getPath().getName());
			}
		}
		
		//3�ر���Դ
		fs.close();
	}
}
