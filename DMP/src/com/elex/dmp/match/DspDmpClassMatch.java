package com.elex.dmp.match;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.elex.dmp.utils.PropertiesUtil;

import edu.cmu.lti.jawjaw.pobj.POS;
import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.lexical_db.data.Concept;
import edu.cmu.lti.ws4j.Relatedness;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.WuPalmer;
import edu.cmu.lti.ws4j.util.WS4JConfiguration;


public class DspDmpClassMatch {
	private  ILexicalDatabase db = new NictWordNet();
	private  RelatednessCalculator rc = new WuPalmer(db);

	/**
	 * @param args
	 * @throws JSONException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, JSONException {
		DspDmpClassMatch ctm = new DspDmpClassMatch();
		List<DspClassDescriptor> result = ctm.readDSPUserClass("d:\\dspUserClass.txt");
		
		for(DspClassDescriptor duc:result){
			System.out.println(duc.getClassId()+","+duc.getClassName()+","+duc.getKeyWords().length);
		}
	}
	
	/*
	 * 对dspUserClassFile的格式有如下要求：
	 * 1、存储为json格式。
	 * 2、每行描述一个dsp的用户类别，每个类别有三个属性，classId，className，keyWords
	 * 3、classId为类别id，为整数标识，不能重复
	 * 4、className为类别名称，英文字符串
	 * 5、keyWords该类的关键词列表，json数组，keyWords可以和名称描述的单词相同
	 * 例：{classId:1,className:"Education",keyWords:["education"]}
	 */
	public List<DspClassDescriptor> readDSPUserClass(String dspUserClassFile) throws IOException, JSONException{
		
		BufferedReader br = new BufferedReader(new FileReader(new File(dspUserClassFile)));
		List<DspClassDescriptor> result = new ArrayList<DspClassDescriptor>();
		
		String line = null ;
		
		while( (line = br.readLine())!= null ){
			JSONObject jsonObject = new JSONObject(line);
			DspClassDescriptor duc = new DspClassDescriptor();
			duc.setClassId(jsonObject.getString("classId"));
			duc.setClassName(jsonObject.getString("className"));
			JSONArray keyWords = jsonObject.getJSONArray("keyWords");
			StringBuffer kw = new StringBuffer();
			for(int i=0;i<keyWords.length();i++){
				kw.append(keyWords.getString(i)+",");
			}
			duc.setKeyWords(kw.toString().substring(0, kw.length()-1).split(","));
			result.add(duc);
			}
		br.close();		
		return result;
		
	}
	
	
	/*
	 * 计算dsp用户分类与dmp用户聚类之间的两两相似度，并建立相互之间的映射关系
	 */
	public void buildMatch(List<DspClassDescriptor> dspClass,List<DmpTopicDescriptor> dmpTopics){
		double wordSim,vectorSim;
		double sim;
		List<Double> simList = new ArrayList<Double>();
		List<TFWord> vector;
		String[] keyWords;
		List<Mapping> dspClassMap;
		List<Mapping> dmpTopicMap;
		
		//BufferedWriter out = new BufferedWriter(new FileWriter("d:\\test.txt"));//调试
		//DecimalFormat df = new DecimalFormat("0.0000000000");//调试
		
		for(DmpTopicDescriptor topic:dmpTopics){
			
			for(DspClassDescriptor cls:dspClass){
				vector = topic.getWordvector();
				keyWords = cls.getKeyWords();
				simList.clear();
				sim = 0D;
				
				for(String word:keyWords){
					wordSim = 0D;
					vectorSim = 0D;
					for(TFWord w:vector){
						//单词相似度以语义网的WU词义相似度衡量再乘以权重
						//wordSim = getWordSim(w.getWord(), word);//计算太耗时
						wordSim = rc.calcRelatednessOfWords(w.getWord(), word);
						if(wordSim>1){
							wordSim=1;
						}
						vectorSim += wordSim*w.getWeight();
						
						//out.write(topic.getTopicID()+","+cls.getClassId()+","+w.getWord()+","+word+","+df.format(wordSim)+","+df.format(vectorSim)+"\r\n");//调试
					}
					//由于topic的特征词是以配置文件中的比例截取的，所以需要还原
					vectorSim = vectorSim/(Double.parseDouble(PropertiesUtil.getPercentage())/100D);	
					simList.add(vectorSim);
										
				}
				
				//如果dsp分类有多个关键词，则取相似度平均值
				for(double unit:simList){
					sim += unit;					
				}
				sim = sim/(double)simList.size();
				
				
				//给DmpTopicDescriptor对象的dspClassMap属性赋值
				if(topic.getDspClassMap() != null){
					dspClassMap = topic.getDspClassMap();
				}else{
					dspClassMap = new ArrayList<Mapping>();
				}
				
				dspClassMap.add(new Mapping(cls.getClassId(), sim));
				topic.setDspClassMap(dspClassMap);
				
				
				//给DspClassDescriptor对象的dmpTopicMap属性赋值
				if(cls.getDmpTopicMap() != null){
					dmpTopicMap = cls.getDmpTopicMap();
				}else{
					dmpTopicMap = new ArrayList<Mapping>();
				}
				
				dmpTopicMap.add(new Mapping(topic.getTopicID(), sim));
				cls.setDmpTopicMap(dmpTopicMap);
								
				
			}
		}
		
		//out.close();//调试
		
	}
	
	
	/*
	 *分别将 DspClassDescriptor和DmpTopicDescriptor对象中的相互间的映射关系属性按照相似度重高到低排序，并将相似度最高的映射对象赋予match
	 */
	public void computeMostMatch(List<DspClassDescriptor> dspClsList,List<DmpTopicDescriptor> dmpTopics){
		for(DmpTopicDescriptor td:dmpTopics){
			Collections.sort(td.getDspClassMap(),new myComparator());	
			td.setMatch(getDspClsById(dspClsList,td.getDspClassMap().get(0).getId()));
		}
		
		for(DspClassDescriptor cls:dspClsList){
			Collections.sort(cls.getDmpTopicMap(), new myComparator()); 						
			cls.setMatch(getTopicById(dmpTopics,cls.getDmpTopicMap().get(0).getId()));
		}
	}
	
	
	public static DmpTopicDescriptor getTopicById(List<DmpTopicDescriptor> dmpTopics,String id){
		for(DmpTopicDescriptor td:dmpTopics){
			if(td.getTopicID()==id){
				return td;
			}
		}
		return null;
	}
	
	
	public static DspClassDescriptor getDspClsById(List<DspClassDescriptor> dspClsList,String id){
		for(DspClassDescriptor cls:dspClsList){
			if(cls.getClassId()==id){
				return cls;
			}			
		}
		return null;
	}
	
	
	public  double getWordSim(String word1,String word2){
		WS4JConfiguration.getInstance().setMFS(true);
		List<POS[]> posPairs = rc.getPOSPairs();
		double maxScore = -1D;

		for(POS[] posPair: posPairs) {
		    List<Concept> synsets1 = (List<Concept>)db.getAllConcepts(word1, posPair[0].toString());
		    List<Concept> synsets2 = (List<Concept>)db.getAllConcepts(word2, posPair[1].toString());

		    for(Concept synset1: synsets1) {
		        for (Concept synset2: synsets2) {
		            Relatedness relatedness = rc.calcRelatednessOfSynset(synset1, synset2);
		            double score = relatedness.getScore();
		            if (score > maxScore) { 
		                maxScore = score;
		            }
		        }
		    }
		}

		if (maxScore == -1D) {
		    maxScore = 0.0;
		}
		return maxScore;
		
	}
	
	class myComparator implements  Comparator<Mapping>{

		@Override
		public int compare(Mapping o1, Mapping o2) {
			int c;
	    	double comp = o2.getWeight() - o1.getWeight();
	    	if(comp>0){
	    		c = 1;
	    	}else if(comp < 0){
	    		c = -1;
	    	}else{
	    		c = 0;
	    	}
	    	return c;
		}
		
	}
		

}
