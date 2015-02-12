package com.elex.dmp.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseOperator {
	/*
	 * 按rowkey前缀查询
	 */
	public List<Map<String,String>> QueryByPrefixRowkey(String tableName, byte[] prefix) {
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();

		try {

			HTableInterface table =  HbaseBasis.getHTablePool().getTable(tableName);
			Scan scan = new Scan();
			scan.setCaching(100);
			scan.setBatch(100);
			scan.setFilter(new PrefixFilter(prefix));
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				if(!r.isEmpty()){
					Map<String,String> map = new HashMap<String,String>();
					map.put("rowKey", Bytes.toString(r.getRow()));
					for (KeyValue keyValue : r.raw()) {
						map.put(new String(keyValue.getQualifier()), new String(keyValue.getValue()));
						//System.out.println(Bytes.toString(Bytes.head(r.getRow(), 1)));
						//System.out.println(Bytes.toInt(Bytes.tail(r.getRow(), r.getRow().length)));
					}
					list.add(map);
				}				
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;

	}
	
	
	/**
	 * 根据rowkey查询唯一一条记录
	 * 
	 * @param tableName
	 * @throws IOException 
	 */
	public Map<String,String> queryOneRecord(String tableName,byte[] rowkey) {

		HTableInterface table = HbaseBasis.getHTablePool().getTable(tableName);
		Map<String,String> map = new HashMap<String,String>();
		try {
			Get scan = new Get(rowkey);// 根据rowkey查询
			scan.setMaxVersions();
			Result r = table.get(scan);
			if(!r.isEmpty()){
				map.put("rowKey", new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					map.put(Bytes.toString(keyValue.getQualifier()),Bytes.toString(keyValue.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return map;
	}
	
	/**
	 * 创建普通表
	 * 
	 * @param tableName
	 */
	public void createTable(String tableName,String columnFamily,int maxVer) {
		System.out.println("start create table ......");
		try {
			HBaseAdmin hBaseAdmin = HbaseBasis.getAdmin();
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.setMaxFileSize(500 * 1024 * 1024);
			tableDescriptor.setMemStoreFlushSize(64 * 1024 * 1024);
			
			
			HColumnDescriptor column = new HColumnDescriptor(columnFamily);
			//column.setCompressionType(Compression.GZIP);
			//column.setInMemory(true);
			if(maxVer >= 0){
				column.setMaxVersions(maxVer);
			}			
			tableDescriptor.addFamily(column);
			//表加关键词提取触发器
			/*FileSystem fs = FileSystem.get(new Configuration());
			Path path = new Path(fs.getUri() + Path.SEPARATOR +"myApp/"+"keyWord.jar");
			String value = path.toString() +"|" + KeyWordExtractTrigger.class.getCanonicalName() +"|" + Coprocessor.PRIORITY_HIGHEST+"|";
			tableDescriptor.setValue("COPROCESSOR$1",value);*/
			
			hBaseAdmin.createTable(tableDescriptor,Bytes.toByteArrays("3"));
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end create table ......");
	}
	
	
	/**
	 * 删除一张表,如果有索引，删除索引表及相关索引信息
	 * 
	 * @param tableName
	 * @throws IOException 
	 */
	public void dropTable(String tableName) throws IOException {
		
		HBaseAdmin admin =  HbaseBasis.getAdmin();
		admin.disableTable(tableName);
		admin.deleteTable(tableName);	

	}
	
	/**
	 * 根据 rowkey删除一条记录
	 * 
	 * @param tablename
	 * @param rowkey
	 */
	public void deleteRow(String tablename, String rowkey) {
		try {
			HTableInterface table = HbaseBasis.getHTablePool().getTable(tablename);
			Delete d1 = new Delete(rowkey.getBytes());
			table.delete(d1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 单条件按某列值查询，查询多条记录
	 * 
	 * @param tableName
	 */
	public List<Map<String,String>> QueryByColumn(String tableName, String family,
			String qualifier, String value) {

		List<Map<String,String>> list = new ArrayList<Map<String,String>>(); 
		try {
			HTableInterface table = HbaseBasis.getHTablePool().getTable(tableName);
			FilterList filterList = new FilterList();			 
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family),
					Bytes.toBytes(qualifier), CompareOp.EQUAL,
					Bytes.toBytes(value));
			filterList.addFilter(filter);			 
			Scan s = new Scan();
			s.setBatch(100);
			s.setCaching(100);
			s.addFamily(Bytes.toBytes(family));		
			s.setFilter(filterList);					 
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				Map<String,String> map = new HashMap<String,String>();
				//System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					map.put(new String(keyValue.getQualifier()), new String(keyValue.getValue()));
				}
				list.add(map);
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	/**
	 * 根据rowkey的起止范围查询
	 * 
	 * @param tableName
	 */
	public List<Map> QueryByKyeRange(String tableName, byte[] start,
			byte[] end) {

		List<Map> list = new ArrayList<Map>(); 
		try {
			HTableInterface table =  HbaseBasis.getHTablePool().getTable(tableName);
			FilterList filterList = new FilterList();			 
 
			Scan s = new Scan();
			s.setBatch(100);
			s.setCaching(100);
			s.setStartRow(start);
			s.setStopRow(end);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				Map map = new HashMap();
				//System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					map.put(new String(keyValue.getQualifier()), new String(keyValue.getValue()));
					//System.out.println(r.getRow().length);
				}
				list.add(map);
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

}
