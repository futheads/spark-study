package com.futhead.java.model;

import java.io.Serializable;

public class CategarySearchClickCount implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String daySearchCategary;
	
	private Long clickCout;

	public CategarySearchClickCount() {
		super();
	}

	public CategarySearchClickCount(String daySearchCategary, Long clickCout) {
		super();
		this.daySearchCategary = daySearchCategary;
		this.clickCout = clickCout;
	}

	public String getDaySearchCategary() {
		return daySearchCategary;
	}

	public void setDaySearchCategary(String daySearchCategary) {
		this.daySearchCategary = daySearchCategary;
	}

	public Long getClickCout() {
		return clickCout;
	}

	public void setClickCout(Long clickCout) {
		this.clickCout = clickCout;
	}

}
