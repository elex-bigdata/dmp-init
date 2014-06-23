package com.elex.dmp.match;

import java.io.Serializable;
import java.util.List;

public class DspClassDescriptor implements Serializable {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5910639133755534150L;
	
	
	private String classId;
	private String className;
	private String[] keyWords;
	private List<Mapping> dmpTopicMap;	
	private DmpTopicDescriptor match;
		
	
	public DmpTopicDescriptor getMatch() {
		return match;
	}
	public void setMatch(DmpTopicDescriptor match) {
		this.match = match;
	}
	public List<Mapping> getDmpTopicMap() {
		return dmpTopicMap;
	}
	public void setDmpTopicMap(List<Mapping> dmpTopicMap) {
		this.dmpTopicMap = dmpTopicMap;
	}
	public String getClassId() {
		return classId;
	}
	public void setClassId(String classId) {
		this.classId = classId;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public String[] getKeyWords() {
		return keyWords;
	}
	public void setKeyWords(String[] keyWords) {
		this.keyWords = keyWords;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DspClassDescriptor other = (DspClassDescriptor) obj;
		if (classId != other.classId)
			return false;
		return true;
	}
	
}
