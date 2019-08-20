package cn.edu.sdut.mapreduce.mutualFriends2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MutualFriendsReducer2 extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		StringBuffer sb = new StringBuffer();
		
		for(Text value : values){
			sb.append(value).append(" ");
		}
		
		context.write(key, new Text(new String(sb)));
	}
}
