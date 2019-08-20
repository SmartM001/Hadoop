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
		
		//0获取配置信息
		Configuration conf = new Configuration();
		
		//1获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2拷贝文件数据到集群
		fs.copyFromLocalFile(new Path("F:/xiyou.txt"), new Path("/user/smart/xiyou.txt"));
	}
	
	//获取文件系统
	@Test
	public void getFileSystem() throws IOException, InterruptedException, URISyntaxException {
		
		//0获取配置信息
		Configuration conf = new Configuration();
		
		//1获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2打印文件系统
		System.out.println(fs.toString());
		
		//3关闭资源
		fs.close();
	}
	
	//上传文件
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0获取配置信息
		Configuration conf = new Configuration();
		
		//1获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2执行上传文件命令,true表示是否删除源文件
		fs.copyFromLocalFile(true, new Path("F:/shuihu.txt"), new Path("/user/smart"));
		
		//3关闭资源
		fs.close();
	}
	
	//下载文件
	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0获取配置信息
		Configuration conf = new Configuration();
		
		//1获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2执行下载文件命令,第一种方式不好使，下载下来的文件是空文件，所以，使用第二种带校验和的方式
		//fs.copyToLocalFile(new Path("/user/smart/xiyouji.txt"), new Path("F:/xiyouji.txt"));
		fs.copyToLocalFile(false, new Path("/user/smart/xiyouji.txt"), new Path("F:/xiyouji.txt"), true);
		
		//3关闭资源
		fs.close();
	}
	
	//创建目录
	@Test
	public void mkdirAtHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0获取配置信息
		Configuration conf = new Configuration();
		
		//1获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2执行创建目录操作,可创建多级目录
		//fs.mkdirs(new Path("/user/smart/output"));
		fs.mkdirs(new Path("/user/smart/xiyouji/houzi/sunwukong"));
		
		//3关闭资源
		fs.close();
		
	}
	
	//删除文件
	@Test
	public void deleteAtHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0获取配置信息
		Configuration conf = new Configuration();
		
		//1获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2执行删除文件的操作
		fs.delete(new Path("/user/smart/xiyouji.txt"),true);
		
		//3关闭资源
		fs.close();
	}
	
	//重命名文件
	@Test
	public void renameAtHDFS() throws IOException, InterruptedException, URISyntaxException{
		
		//0获取配置信息
		Configuration conf = new Configuration();
		
		//1获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2执行重命名文件的操作
		fs.rename(new Path("/user/smart/xiyouji"), new Path("/user/smart/xiyou"));
		
		//3关闭资源
		fs.close();
	}
	
	//查看文件详情
	@Test
	public void readFileAtHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0获取配置信息
		Configuration conf = new Configuration();
		
		//1获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2执行查看文件详情操作
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		while(listFiles.hasNext()) {
			
			LocatedFileStatus status = listFiles.next();
			
			// 文件名称
			System.out.println(status.getPath().getName());
			
			// 块大小
			System.out.println(status.getBlockSize());
			
			// 文件内容的长度
			System.out.println(status.getLen());
			
			// 文件权限
			System.out.println(status.getPermission());
			
			System.out.println("-------------------");
			
			// 文件块的具体信息
			BlockLocation[] blockLocations = status.getBlockLocations();
			
			for(BlockLocation block : blockLocations) {
				System.out.println(block.getOffset());
				
				String[] hosts = block.getHosts();
				
				for(String host:hosts) {
					System.out.println(host);
				}
			}
		}
			
		//3关闭资源
		fs.close();
	}
	
	// 获取文件夹和文件信息
	@Test
	public void readFloderAtHDFS() throws IOException, InterruptedException, URISyntaxException {
		
		//0获取配置信息
		Configuration conf = new Configuration();
		
		//1获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "smart");
		
		//2执行获取文件夹和文件信息，判断是文件夹还是文件
		FileStatus[] listStatus = fs.listStatus(new Path("/user/smart/"));
		
		for(FileStatus status : listStatus) {
			
			if(status.isFile()) {
				System.out.println("f---" + status.getPath().getName());
			}else {
				System.out.println("d---" + status.getPath().getName());
			}
		}
		
		//3关闭资源
		fs.close();
	}
}
