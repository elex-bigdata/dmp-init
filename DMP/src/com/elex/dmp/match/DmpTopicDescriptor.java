package com.elex.dmp.match;

import java.io.Serializable;
import java.util.List;

public class DmpTopicDescriptor implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -332387831696008197L;
	
	private String topicID;//主题id
	
	private List<TFWord> wordvector;//特征向量,初始是全部，据配置的权重阈值截取的topN个特征值		
	
	private List<Mapping> dspClassMap;//与dsp各分类的相似度
		
	private DspClassDescriptor match;//与dsp分类相似度最高的分类
	
	
	public DspClassDescriptor getMatch() {
		return match;
	}


	public void setMatch(DspClassDescriptor match) {
		this.match = match;
	}

	public List<Mapping> getDspClassMap() {
		return dspClassMap;
	}


	public void setDspClassMap(List<Mapping> dspClassMap) {
		this.dspClassMap = dspClassMap;
	}

	public String getTopicID() {
		return topicID;
	}
	public void setTopicID(String topicID) {
		this.topicID = topicID;
	}
	public List<TFWord> getWordvector() {
		return wordvector;
	}
	public void setWordvector(List<TFWord> wordvector) {
		this.wordvector = wordvector;
	}
	
	
}
