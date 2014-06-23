package com.elex.dmp.match;

import java.io.Serializable;

public class Mapping implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -391202911744991220L;
	
	private String id;
	private double weight;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}
	
	
	
	public Mapping(String id, double weight) {
		super();
		this.id = id;
		this.weight = weight;
	}
		

}
