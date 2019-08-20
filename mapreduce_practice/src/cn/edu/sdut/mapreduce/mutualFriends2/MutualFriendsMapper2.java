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
		// ���󣺽�mutualfriends1�еĽ��mr�ڶ��Σ��ҵ���Щ������֮���й�ͬ���ѣ��Լ������Ĺ�ͬ���Ѷ���˭
		// 1 ��ȡһ��
		String line = value.toString();
		
		// 2 ��ȡ
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
