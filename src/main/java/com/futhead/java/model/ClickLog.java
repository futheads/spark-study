package com.futhead.java.model;

public class ClickLog implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	
	private String ip;
	private String time;
	private int categaryId;
	private String refer;
	private int statusCode;

	public ClickLog() {
		super();
	}

	public ClickLog(String ip, String time, int categaryId, String refer, int statusCode) {
		super();
		this.ip = ip;
		this.time = time;
		this.categaryId = categaryId;
		this.refer = refer;
		this.statusCode = statusCode;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public int getCategaryId() {
		return categaryId;
	}

	public void setCategaryId(int categaryId) {
		this.categaryId = categaryId;
	}

	public String getRefer() {
		return refer;
	}

	public void setRefer(String refer) {
		this.refer = refer;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	@Override
	public String toString() {
		return "ClickLog [ip=" + ip + ", time=" + time + ", categaryId=" + categaryId + ", refer=" + refer
				+ ", statusCode=" + statusCode + "]";
	}

}
