package cn.edu.sdut.mapreduce.gilivideoETL;

public class ETLUtil {
	
	public static String etlUtil(String line){
		
		// 1 切割数据
		String[] fields = line.split("\t");
		
		// 2 过滤脏数据
		if(fields.length < 9){
			return null;
		}
		
		// 3 去掉类别字段中的空格
		fields[3] = fields[3].replaceAll(" ", "");
		
		// 4 替换关联视频的分隔符
		StringBuffer sb = new StringBuffer();
		
		for(int i=0; i<fields.length; i++){
			if(i<9){
				if(i==fields.length-1){
					sb.append(fields[i]);
				}else{
					sb.append(fields[i]).append("\t");
				}
			}else{
				if(i==fields.length-1){
					sb.append(fields[i]);
				}else{
					sb.append(fields[i]).append("&");
				}
			}
		}
		
		return sb.toString();
	}
}
