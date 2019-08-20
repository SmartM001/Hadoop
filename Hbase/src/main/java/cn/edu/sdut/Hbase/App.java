package cn.edu.sdut.Hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * Hbase简单API操作
 */
public class App 
{
	private static Admin admin = null;
	private static Connection connection = null;
	private static Configuration conf = null;
	
	static{
		
		// 获取配置文件
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop102");
		
		// 获取Hbase管理对象
		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// 关闭资源
	private static void close(Connection connection, Admin admin){
		if(connection!=null){
			try {
				connection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(admin!=null){
			try {
				admin.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	// 判断表是否存在
	public static boolean tableExist(String tableName) throws IOException{
		
		// 1 执行操作
		boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
		
		return tableExists;
	}
	
	// 创建表
	public static void createTable(String tableName, String... cfs) throws IOException{
		
		// 1 判断表是否存在
		if(tableExist(tableName)){
			System.out.println("表已存在!!");
			return;
		}
		
		// 2 创建表描述器
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		
		// 3 向表描述器中添加列族
		for(String cf : cfs)
		{
			// 创建列描述器
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
			
			// 设置列族的版本号
			// hColumnDescriptor.setMaxVersions(2);
			
			// 向列描述器中添加列族
			hTableDescriptor.addFamily(hColumnDescriptor);
		}
		
		// 4 执行创建表操作
		admin.createTable(hTableDescriptor);
		
		System.out.println("表创建成功");
	}
	
	// 删除表
	public static void deleteTable(String tableName) throws IOException{
		
		// 1 判断表是否存在
		if(!tableExist(tableName)){
			System.out.println("表不存在！！");
			return;
		}
		
		// 1 使表不可用
		admin.disableTable(TableName.valueOf(tableName));
		
		// 2 执行删除表操作
		admin.deleteTable(TableName.valueOf(tableName));
		
		System.out.println("表已删除！！");
	}
	
	// 增加/修改数据
	public static void putData(String tableName, String rowKey, String cf, String col, String value) throws IOException{
		
		// 1 获取表名
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		// 2 创建put对象
		Put put = new Put(Bytes.toBytes(rowKey));
		// 3 添加数据
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
		
		// 4 执行插入操作
		table.put(put);
		
		// 5 关闭资源
		table.close();
	}
	
	// 删除数据
	public static void deleteData(String tableName, String rowKey, String cf, String col) throws IOException{
		
		// 1 获取表名
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		// 2 创建delete对象
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		//删除所有版本的数据
		delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(col));
		/*
		 *  删除最新版本的数据
		 *  如果是多版本的时候可以用
		 *  但是如果只有一个版本，不建议用，因为删除最新版本后，旧版本又回来了
		 *  delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cs));
		 */
		
		// 3 执行删除操作
		table.delete(delete);
		
		// 4 关闭资源
		table.close();
	}
	
	// 查询数据之遍历表格
	public static void scanTable(String tableName) throws IOException{
		
		// 1 获取表名
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		// 2 创建scanner对象
		Scan scan = new Scan();
		
		// 3 执行遍历操作，获取所有rowkey
		ResultScanner results = table.getScanner(scan);
		
		// 4 将遍历的数据打印到控制台上,result是每一个rowkey
		for(Result result : results){
			// cell是每一个rowkey中的单元格
			Cell[] cells = result.rawCells();
			for(Cell cell : cells){
				System.out.println(
						"RK:" + Bytes.toString(CellUtil.cloneRow(cell))
						+ ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
						+ ",Col:" + Bytes.toString(CellUtil.cloneQualifier(cell))
						+ ",CF:" + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		
		// 5 关闭资源
		table.close();
	}
	
	// 查询数据之获取指定数据
	public static void getData(String tableName, String rowKey, String cf, String col) throws IOException{
		
		//获取表名
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		// 创建get对象，获取当前rowkey
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col));
		// get.setMaxVersions(); 设置版本号
		
		// 执行查询操作
		Result results = table.get(get);
		Cell[] cells = results.rawCells();
		for(Cell cell : cells){
			System.out.println(
					"RK:" + Bytes.toString(CellUtil.cloneRow(cell))
					+ ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
					+ ",Col:" + Bytes.toString(CellUtil.cloneQualifier(cell))
					+ ",CF:" + Bytes.toString(CellUtil.cloneValue(cell)));
		}
		
		// 关闭资源
		table.close();
	}
	
    public static void main( String[] args ) throws IOException
    {
//    	System.out.println(tableExist("student"));
//    	System.out.println(tableExist("staff"));
    	
//    	createTable("staff", "info");
//    	System.out.println(tableExist("staff"));
    	
//    	deleteTable("staff");
//    	System.out.println(tableExist("staff"));
    	
//    	putData("student", "1001", "info", "name", "xiaoming");
    	
//    	deleteData("student", "1001", "info", "name");
//    	scanTable("student");
    	
    	getData("student", "1001", "info", "age");
    	
    	close(connection, admin);
    	
    	
    }
}
