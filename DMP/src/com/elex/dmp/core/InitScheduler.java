package com.elex.dmp.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elex.dmp.lda.CVB0Driver;
import com.elex.dmp.match.BuildMatchThread;
import com.elex.dmp.match.DmpTopicDescriptor;
import com.elex.dmp.match.DspClassDescriptor;
import com.elex.dmp.match.DspDmpClassMatch;
import com.elex.dmp.match.LDAParser;
import com.elex.dmp.utils.HdfsUtils;
import com.elex.dmp.utils.PropertiesUtil;
import com.elex.dmp.vectorizer.SparseVectorsFromSequenceFiles;

public class InitScheduler {

	private static final Logger log = LoggerFactory.getLogger(InitScheduler.class);
	

	/**
	 * @param args
	 * args[0]为开始阶段标识
	 * args[1]为结束阶段标识
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		AtomicInteger currentPhase = new AtomicInteger();
		String[] stageArgs = {otherArgs[0],otherArgs[1]};//运行阶段控制参数
		String[] userFeatureArgs = {PropertiesUtil.getTimeSpanMin(),PropertiesUtil.getUserFeatureOut()};
		
		//userFeatureExtract方法的输出目的地为genTFIDFVector方法的输入路径
		String[] vectorArgs = {PropertiesUtil.getUserFeatureOut(),PropertiesUtil.getVectorOut()};
		
		String[] cluseterArgs = {PropertiesUtil.getVectorOut(),PropertiesUtil.getClusterOut(),
				                 PropertiesUtil.getDocTopicOutput(),PropertiesUtil.getNumTopics(),
				                 PropertiesUtil.getClusterMaxIter(),PropertiesUtil.getVectorOut()+"/dictionary.file-0",
				                 PropertiesUtil.getCvbTemp()};
		
		String[] customizeArgs ={PropertiesUtil.getVectorOut()+"/dictionary.file-0",PropertiesUtil.getClusterOut(),
				                 PropertiesUtil.getPercentage(),PropertiesUtil.getModelFilePath(),
				                 PropertiesUtil.getDspUserClassFile()};
		
		String[] indexArgs = {PropertiesUtil.getDocTopicOutput()};
				
		int success = 0;		
		
		//stage 0
		if (shouldRunNextPhase(stageArgs, currentPhase)){
			log.info("开始提取用户特征文件！");
			success = userFeatureExtract(userFeatureArgs);
			if(success != 0){
				log.error("提取用户特征文件出错，系统退出！");
				System.exit(success);
			}
			log.info("结束提取用户特征文件！");
		}
		
		//stage 1
		if (shouldRunNextPhase(stageArgs, currentPhase)){
			log.info("开始将用户特征文件转为向量！");
			success = genTFIDFVector(vectorArgs);
			if(success != 0){
				log.error("将用户特征文件转为向量出错，系统退出");
				System.exit(success);
			}
			log.info("结束将用户特征文件转为向量！");
		}
		
		
		//stage 2
		if (shouldRunNextPhase(stageArgs, currentPhase)){
			log.info("开始进行用户聚类！");
			success = userCluseter(cluseterArgs);
			if(success != 0){
				log.error("用户聚类出错，系统退出！");
				System.exit(success);
			}
			log.info("结束用户聚类！");
		}
		
		//stage 3
		if (shouldRunNextPhase(stageArgs, currentPhase)){
			log.info("开始建立dmp和dsp分类的映射关系！");
			success = customizeMutipleThread(customizeArgs);//使用多线程计算
			//success = customize(customizeArgs);//使用单线程计算
			if(success != 0){
				log.error("建立dmp和dsp分类的映射关系出错，系统退出！");
				System.exit(success);
			}
			log.info("建立dmp和dsp分类的映射关系结束！");
		}
		
		//stage 4
		if (shouldRunNextPhase(stageArgs, currentPhase)){
			log.info("开始建立用户与dmp分类的索引！");
			success = buildIndex(indexArgs);
			if(success != 0){
				log.error("建立索引出错，系统退出！");
				System.exit(success);
			}
			log.info("结束建立用户与dmp索引！");
		}
		
		//stage 5
		if (shouldRunNextPhase(stageArgs, currentPhase)){
			log.info("开始清理和备份！");
			success = cleanAndBackup();
			if(success != 0){
				log.error("清理和备份出错，系统退出！");
				System.exit(success);
			}
			log.info("结束清理和备份！");
		}
										
	}
	
	/*
	 * 0=备份目录（backUpDir）
	 * 1=临时状态目录（state_temp）
	 * 2=迭代次数（clusterMaxIter）
	 * 3=tf向量目录（vectorOut）
	 * 6=dsp和dmp分类映射文件（modelFile）
	 * 4=聚类结果目录（clusterOut）
	 * 5=doc-topic目录（doc_topic_output）
	 * 7=特征文件目录（userFeatureOut）
	 */
	private static int cleanAndBackup() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		HdfsUtils.makeDir(fs,PropertiesUtil.getBackUpDir());
		HdfsUtils.makeDir(fs,PropertiesUtil.getBackUpDir()+"/dict");
		HdfsUtils.makeDir(fs,PropertiesUtil.getBackUpDir()+"/model");
		Path finalModelPath = CVB0Driver.modelPath(new Path(PropertiesUtil.getCvbTemp()), Integer.parseInt(PropertiesUtil.getClusterMaxIter()));
		
		
		FileStatus[] modelFiles = fs.listStatus(finalModelPath,PathFilters.partFilter());
		
		for (int i = 0; i < modelFiles.length; i++) {
			HdfsUtils.backupFile(fs,conf,modelFiles[i].getPath().toString(), PropertiesUtil.getBackUpDir()+"/model/"+modelFiles[i].getPath().getName());
		}
		
		FileStatus[] dictFiles = fs.listStatus(new Path(PropertiesUtil.getVectorOut()),new PathFilter() {
		    @Override
		    public boolean accept(Path path) {
		      String name = path.getName();
		      return name.startsWith("dictionary.") && !name.endsWith(".crc");
		    }
		  });
		
		for (int i = 0; i < dictFiles.length; i++) {
			HdfsUtils.backupFile(fs,conf,dictFiles[i].getPath().toString(), PropertiesUtil.getBackUpDir()+"/dict/"+dictFiles[i].getPath().getName());
		}
		
		FileStatus[] clusterFile = fs.listStatus(new Path(PropertiesUtil.getClusterOut()),new PathFilter(){

			@Override
			public boolean accept(Path path) {				
				return path.getName().startsWith("part-m-");
			}
			
		});
				
		for (int i = 0; i < clusterFile.length; i++) {
			HdfsUtils.backupFile(fs,conf,clusterFile[i].getPath().toString(), PropertiesUtil.getBackUpDir()+"/cluster/"+clusterFile[i].getPath().getName());
		}
		
		HdfsUtils.backupFile(fs,conf,PropertiesUtil.getModelFilePath(),PropertiesUtil.getBackUpDir()+PropertiesUtil.getModelFilePath());
		HdfsUtils.delFile(fs, PropertiesUtil.getCvbTemp());
		HdfsUtils.delFile(fs, PropertiesUtil.getVectorOut());
		HdfsUtils.delFile(fs, PropertiesUtil.getClusterOut());
		HdfsUtils.delFile(fs, PropertiesUtil.getDocTopicOutput());
		HdfsUtils.delFile(fs, PropertiesUtil.getModelFilePath());
		HdfsUtils.delFile(fs, PropertiesUtil.getUserFeatureOut());
		fs.close();
		
		return 0;
	}


	/*
	 * 本方法所用的SparseVectorsFromSequenceFiles类是私有的，原因在于mahout0.7基于的mapreduce版本不一致，
	 * 在cdh2.0中运行时该类调用的TFPartialVectorReducer的110行会报错：得到的是接口而不是类
	 * 
	 */
	public static int genTFIDFVector(String args[]) throws Exception{
		String[] newArgs = {"-i", args[0], "-o", args[1], "-ow", 
				"-chunk", "128", "-wt","tf", "-s", "3", "-md",
				"2", "-x", "70", "-ng", "1", "-ml", "50", "-seq", "-n", "2"};
		return ToolRunner.run(new SparseVectorsFromSequenceFiles(), newArgs);
	}
	
	
	
	
	public static int userFeatureExtract(String args[]) throws Exception{		 
		 return ToolRunner.run(new Configuration(), new UserFeatureExtract(), args);
	}
	
	/*
	 * mahout0.7中的cvb的输入要求是seqfile，key为IntWriatble,本工程将cvb进行了改造使其适应key为Text的情况。
	 */
	public static int userCluseter(String[] args) throws Exception{		
		int b =ToolRunner.run(new Configuration(), new CVB0Driver(), new String[]{
			"-i",args[0]+"/tf-vectors",
			"-o",args[1],
			"-dt",args[2],
			"-k",args[3],
			"-x",args[4],
			"-ow","-dict",
			args[5],
			"--topic_model_temp_dir",args[6]});//如果不指定且不清理临时文件，程序将报错 illegal state。。。
		return b==0?0:1;
	}
	
	
	
	public static int customize(String[] args) throws IOException, JSONException{
		LDAParser  parser = new LDAParser();
		DspDmpClassMatch match = new DspDmpClassMatch();
		
		List<DmpTopicDescriptor> topics = parser.initTopics(args);
		parser.findTopN(args[2],topics);
		
		
		List<DspClassDescriptor> dspClsList = match.readDSPUserClass(args[4]);
		match.buildMatch(dspClsList, topics);
		match.computeMostMatch(dspClsList, topics);
		
		parser.ldaWriter(dspClsList,topics,args[3]);
		
		return 0;
		
	}
	
	/*
	 * 使用多线程计算dsp用户分类与dmp用户聚类之间的相似度并建立相互之间的映射关系
	 */
	public static int customizeMutipleThread(String[] args) throws IOException, JSONException, InterruptedException{
		int poolSize = new Integer(PropertiesUtil.getPoolSize());//线程池的容量，在配置文件中指定
		int start,end;//每个线程处理dmp聚类的起止对象数
		ExecutorService pool = Executors.newFixedThreadPool(poolSize);//初始化一个固定容量的线程池
		CountDownLatch latch=new CountDownLatch(poolSize);
		
		LDAParser  parser = new LDAParser();
		DspDmpClassMatch match = new DspDmpClassMatch();
		
		List<DmpTopicDescriptor> topics = parser.initTopics(args);
		List<List<DmpTopicDescriptor>> dmptopicList = new ArrayList<List<DmpTopicDescriptor>>();
		parser.findTopN(args[2],topics);
		parser.describeCluster(topics, "Cluster-describe.txt");
		
		
		int avg = (topics.size()/poolSize)+1;//每个线程最多处理的topic数
		
				
		List<DspClassDescriptor> dspClsList = match.readDSPUserClass(args[4]);
		
		for(int i=0;i<poolSize;i++){
			start = (i*avg)>topics.size()?topics.size():i*avg;
			end  = (i*avg+avg)>topics.size()?topics.size():(i*avg+avg);
			dmptopicList.add(topics.subList(start, end));
			BuildMatchThread t = new BuildMatchThread(dspClsList,dmptopicList.get(i),latch);
			pool.submit(t);			
		}				
		latch.await();//将各个线程处理的结果合并
		//以下代码没有必要，注释掉。因为dmptopicList里的元素和topics里的元素是同一批对象
		/*List<DmpTopicDescriptor> dmpList = new ArrayList<DmpTopicDescriptor>();
		for(List<DmpTopicDescriptor> list:dmptopicList){
			dmpList.addAll(list);			
		}*/
		
		match.computeMostMatch(dspClsList, topics);
		
		parser.ldaWriter(dspClsList,topics,args[3]);
		pool.shutdown();
		
		return 0;
		
	}
	
	public static int buildIndex(String args[]) throws Exception{
		return ToolRunner.run(new Configuration(), new UserClassIndexBuiler(),args);
	}
	
	protected static boolean shouldRunNextPhase(String[] args, AtomicInteger currentPhase) {
	    int phase = currentPhase.getAndIncrement();
	    String startPhase = args[0];
	    String endPhase = args[1];
	    boolean phaseSkipped = (startPhase != null && phase < Integer.parseInt(startPhase))
	        || (endPhase != null && phase > Integer.parseInt(endPhase));
	    if (phaseSkipped) {
	      log.info("Skipping phase {}", phase);
	    }
	    return !phaseSkipped;
	  }
	
	
}
