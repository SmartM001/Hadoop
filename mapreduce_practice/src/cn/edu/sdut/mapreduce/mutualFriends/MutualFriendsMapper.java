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
		// 1 获取一行
		String line = value.toString();
		
		// 2 截取,获取person和friends
		String[] fields = line.split(":");
		String person = fields[0];
		text1.set(person);
		// 得到每一个friend
		String[] friends = fields[1].split(",");
		
		for(String friend : friends){
			// 3 写出<friend,person>
			context.write(new Text(friend), text1);
		}		
	}
}
