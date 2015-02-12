package com.elex.dmp.hbase;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.mahout.common.RandomUtils;

import com.elex.dmp.utils.RandomStringUtils;


public class HbaseInit {

	/**
	 * 
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {	
		HbaseOperator operator = new HbaseOperator();
		//createTable();
		//generateTestData();
		/*byte[] type =new byte[]{PropertiesUtil.getType()};
        long now =System.currentTimeMillis();
        long stop = now - Long.parseLong(PropertiesUtil.getTimeSpanMin())*60*1000;
        byte[] startRow = Bytes.add(type,Bytes.toBytes(stop),Bytes.toBytes("0"));
        byte[] stopRow = Bytes.add(type,Bytes.toBytes(now),Bytes.toBytes("0"));   
		System.out.println(operator.QueryByKyeRange("dmp_user_action", startRow, stopRow).size());*/
		/*byte[] start = Bytes.toBytes("F");
		byte[] stop = Bytes.toBytes("G");*/
		//System.out.println(operator.QueryByPrefixRowkey("dmp_url_detail",Bytes.toBytes("F")).size());
		operator.queryOneRecord("dmp_user_classify", Bytes.toBytes("2n8a"));
		
		
	}
	
	public static void createTable(){
		HbaseOperator operator = new HbaseOperator();
		operator.createTable("dmp_url_detail", "ud", 1);
		//ud:T(title),ud:M(metainfo),ud:L(language),ud:K(关键词列表),rowkey为(F+url)初始前缀为F，提取完关键字后为(T+url)
		operator.createTable("dmp_user_action", "ua", 1);
		//ua:url(用户访问的url)，rowkey为时间戳和用户硬盘编号
		operator.createTable("dmp_user_classify", "uc", 3);
		//uc:AC(用户访问url的有效数量),uc:dt(用户与dmp聚类的概率分布)，uc：M(用户与对应的dsp分类)，rowkey为用户id的BKDR哈希值
		//operator.createTable("dmp_uid_mapping", "um", 1);//um:uid(用户id即硬盘编号)，rowkey为用户id的BKDR哈希值
	}
	
	public static void generateTestData() throws IOException, InterruptedException{
		
		String url;
		HTableInterface ud = HbaseBasis.getHTablePool().getTable("dmp_url_detail");
		HTableInterface ua = HbaseBasis.getHTablePool().getTable("dmp_user_action");
		BufferedReader odp = new BufferedReader(new FileReader("D:\\elex-tech\\app\\dmp\\urls.txt"));
		BufferedReader titleFile = new BufferedReader(new FileReader("D:\\elex-tech\\app\\dmp\\title.txt.bak"));
		String[] TM;
		byte[] type =new byte[]{1};
		long timestamp;
		byte[] rowKey;
		String uid;
		for(int i=0;i<3000;i++){			
			uid = RandomStringUtils.generateMixString(4);			
			int round = RandomUtils.getRandom().nextInt(25)+1;
			
			for(int j=0;j<round;j++){
				timestamp =System.currentTimeMillis();
				Thread.sleep(1);
				rowKey = Bytes.add(type,Bytes.toBytes(timestamp),Bytes.toBytes(uid));
				url = odp.readLine();
				TM = titleFile.readLine().split("===");
				Put uaPut = new Put(rowKey);
				uaPut.add(Bytes.toBytes("ua"), Bytes.toBytes("url"), Bytes.toBytes(url));
				uaPut.setWriteToWAL(false);
				ua.setAutoFlush(false);
				ua.put(uaPut);
				
				byte[] drowPrefix = Bytes.toBytes("F");
				Put udPut = new Put(Bytes.add(drowPrefix,Bytes.toBytes(url)));
				udPut.add(Bytes.toBytes("ud"),Bytes.toBytes("t"), Bytes.toBytes(TM[0]));
				udPut.add(Bytes.toBytes("ud"),Bytes.toBytes("m"), Bytes.toBytes(TM[1]));
				udPut.setWriteToWAL(false);
				ud.setAutoFlush(false);
				ud.put(udPut);
			}
		}
		odp.close();
		titleFile.close();
		ua.close();
		ud.close();
		
		
	}


}
