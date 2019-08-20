package cn.edu.sdut.mapreduce.mutualFriends2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MutualFriendsMapper2 extends Mapper<LongWritable, Text, Text, Text>{
	
	Text text = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 需求：将mutualfriends1中的结果mr第二次，找到哪些人两两之间有共同好友，以及他俩的共同好友都有谁
		// 1 获取一行
		String line = value.toString();
		
		// 2 截取
		String[] fields = line.split("\t");
		String person = fields[0];
		text.set(person);
		
		String[] friends = fields[1].split(",");
		
		Arrays.sort(friends);
		
		for(int i=0;i<friends.length-1;i++){
			for(int j=i+1;j<friends.length;j++){
				context.write(new Text(friends[i]+"-"+friends[j]), text);
			}
		}
	}
}
