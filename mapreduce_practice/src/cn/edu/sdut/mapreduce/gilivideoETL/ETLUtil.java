package cn.edu.sdut.mapreduce.gilivideoETL;

public class ETLUtil {
	
	public static String etlUtil(String line){
		
		// 1 �и�����
		String[] fields = line.split("\t");
		
		// 2 ����������
		if(fields.length < 9){
			return null;
		}
		
		// 3 ȥ������ֶ��еĿո�
		fields[3] = fields[3].replaceAll(" ", "");
		
		// 4 �滻������Ƶ�ķָ���
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
