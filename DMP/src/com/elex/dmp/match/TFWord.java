package com.elex.dmp.match;

import java.io.Serializable;

public class TFWord implements Serializable,Comparable<TFWord>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 9012382319857541683L;
	
	private String word;
	private double weight;
	
	public String getWord() {
		return word;
	}


	public void setWord(String word) {
		this.word = word;
	}


	public double getWeight() {
		return weight;
	}


	public void setWeight(double weight) {
		this.weight = weight;
	}	
	
	public TFWord(String word, double weight) {
		super();
		this.word = word;
		this.weight = weight;
	}


	@Override
	public int compareTo(TFWord o) {
		if(o.getWeight()>this.weight){
			return 1;
			
		}else if(o.getWeight()<this.weight){
			return -1;
		}else{
			return 0;
		}
		
	}
	
	
	
	

}
