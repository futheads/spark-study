package com.futhead.java.model;

public class CategaryClickCount {

	private String categaryID;
	
	private Long clickCout;

	public CategaryClickCount() {
		super();
	}

	public CategaryClickCount(String categaryID, Long clickCout) {
		super();
		this.categaryID = categaryID;
		this.clickCout = clickCout;
	}

	public String getCategaryID() {
		return categaryID;
	}

	public void setCategaryID(String categaryID) {
		this.categaryID = categaryID;
	}

	public Long getClickCout() {
		return clickCout;
	}

	public void setClickCout(Long clickCout) {
		this.clickCout = clickCout;
	}
	
}
