package cn.edu.sdut.mapreduce.mutualFriends;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MutualFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	Text text1 = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 ��ȡһ��
		String line = value.toString();
		
		// 2 ��ȡ,��ȡperson��friends
		String[] fields = line.split(":");
		String person = fields[0];
		text1.set(person);
		// �õ�ÿһ��friend
		String[] friends = fields[1].split(",");
		
		for(String friend : friends){
			// 3 д��<friend,person>
			context.write(new Text(friend), text1);
		}		
	}
}
