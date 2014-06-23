package com.elex.dmp.match;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class LDAParser {

	/**
	 * @param args
	 * args[0]=字典路径
	 * args[1]=聚类结果文件路径通配字符串，如/cluster/*
	 * args[2]=topN的权重之和
	 * args[3]=topics的输出本地输出路径
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		LDAParser  parser = new LDAParser();
		List<DmpTopicDescriptor> topics = parser.initTopics(new String[]{"/dmp-bak/dict/dictionary.file-0","/dmp-bak/cluster"});
		parser.findTopN("80",topics);
		parser.describeCluster(topics, "D:\\describe.txt");
		
	}
	
	/**
	 * 
	 * @param dspClsList 
	 * @param topics 
	 * @param destFile 本地写入路径
	 * @throws IOException
	 */
	public void ldaWriter(List<DspClassDescriptor> dspClsList, List<DmpTopicDescriptor> topics,String destFile) throws IOException{		
		DecimalFormat df = new DecimalFormat("0.0000000000");				
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(destFile);
		FSDataOutputStream out = fs.create(path);
	    //String topn;
	    String dspMap;
	    String dmpMap;
		for(DmpTopicDescriptor topic:topics){
			//StringBuffer topNStr = new StringBuffer();
			StringBuffer dspMapStr = new StringBuffer();
			/*topNStr.append("[");
			for(TFWord w:topic.getWordvector()){
				topNStr.append(w.getWord()+":"+df.format(w.getWeight())+",");
			}*/
			dspMapStr.append("[");
			for(Mapping m:topic.getDspClassMap()){
				dspMapStr.append("{");
				dspMapStr.append("\""+m.getId()+"\""+":"+df.format(m.getWeight())+"},");
			}
			//topn = topNStr.substring(0, topNStr.length()-1)+"]";
			dspMap = dspMapStr.substring(0, dspMapStr.length()-1)+"]";
			//out.write("{"+"id:"+topic.getTopicID()+","+"dspMap:"+dspMap+"topN:"+topn+"}\r\n");
			out.write(Bytes.toBytes(new String("{"+"\"id\":"+topic.getTopicID()+","+"\"dspMap\":"+dspMap+"}\r\n")));
		}
		
		for(DspClassDescriptor cls:dspClsList){
			StringBuffer dmpMapStr = new StringBuffer();
			
			dmpMapStr.append("[");
			for(Mapping m:cls.getDmpTopicMap()){
				dmpMapStr.append("{");
				dmpMapStr.append("\""+m.getId()+"\""+":"+df.format(m.getWeight())+"},");
			}
			dmpMap = dmpMapStr.substring(0, dmpMapStr.length()-1)+"]";
			out.write(Bytes.toBytes(new String("{"+"\"id\":"+cls.getClassId()+","+"\"dmpMap\":"+dmpMap+"}\r\n")));
		}
		
		out.close();
				
	}
	

	/**
	 * 
	 * @param dictFile 字典文件路径
	 * @return
	 * @throws IOException
	 */
	private Map<Integer,String> getDictionary(String dictFile)
			throws IOException {
		Map<Integer,String> dict = new HashMap<Integer,String>();
		Configuration conf = new Configuration();
		Path path = new Path(dictFile);
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,Reader.file(path));
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while(reader.next(key,value)){
			dict.put(Integer.parseInt(value.toString()),key.toString());
		}
		reader.close();
		return dict;

	}
	
	/**
	 * 
	 * @param args[0]为字典文件路径，args[1]为聚类结果路径例如/cluter/*
	 * @return
	 * @throws IOException
	 */
	public List<DmpTopicDescriptor> initTopics(String[] args) throws IOException{
		List<DmpTopicDescriptor> result = new ArrayList<DmpTopicDescriptor>();
		List<TFWord> topic = null;
		Iterator<Vector.Element> nonZeroElements = null;
		Vector.Element nonZeroElement = null;		
		Map<Integer,String> dictionary = getDictionary(args[0]);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(args[1]);
		FileStatus[] files = fs.listStatus(path,new PathFilter(){

			@Override
			public boolean accept(Path path) {				
				return path.getName().startsWith("part-m-");
			}
			
		});
		
		for(FileStatus file:files){
			SequenceFile.Reader reader = new SequenceFile.Reader(conf,Reader.file(file.getPath()));
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			VectorWritable value = (VectorWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while(reader.next(key,value)){				
				DmpTopicDescriptor td = new DmpTopicDescriptor();
				topic = new ArrayList<TFWord>();
				nonZeroElements = value.get().iterateNonZero();
				td.setTopicID(key.toString());
				while (nonZeroElements.hasNext()) {
					nonZeroElement = nonZeroElements.next();
					topic.add(new TFWord(dictionary.get(nonZeroElement.index()),  nonZeroElement.get()));
				}
				td.setWordvector(topic);
				result.add(td);
			}
			
			IOUtils.closeStream(reader);
		}
		return result;
	}
	
	/**
	 * 
	 * @param comparePercent 权重和
	 * @param topics 主题的词频向量
	 * @throws IOException
	 */
	public void findTopN(String comparePercent,List<DmpTopicDescriptor> topics) throws IOException{
				
		double percent = Double.parseDouble(comparePercent)/100D;
		double topicPercent;
		int toIndex;
		for(DmpTopicDescriptor td:topics){
			topicPercent = 0D;
			toIndex = 0;
			Collections.sort(td.getWordvector()); 
			
			for(TFWord v:td.getWordvector()){				
				topicPercent += v.getWeight();
				toIndex++;
				
				if(topicPercent >= percent){
					td.setWordvector(td.getWordvector().subList(0, toIndex));
					break;
				}				
			}
		}
			
	}
	
	public void describeCluster(List<DmpTopicDescriptor> topics,String destFile) throws IOException{
		DecimalFormat df = new DecimalFormat("0.0000000000");				
		BufferedWriter out = new BufferedWriter(new FileWriter(destFile));
	    String topn;
	    
		for(DmpTopicDescriptor topic:topics){
			StringBuffer topNStr = new StringBuffer();
			topNStr.append("[");
			for(TFWord w:topic.getWordvector()){
				topNStr.append("{");
				topNStr.append(w.getWord()+":"+df.format(w.getWeight())+"},");
			}
			
			topn = topNStr.substring(0, topNStr.length()-1)+"]";
			out.write("{"+"id:"+topic.getTopicID()+","+"topN:"+topn+"}\r\n");
		}
						
		out.close();
	}

}
