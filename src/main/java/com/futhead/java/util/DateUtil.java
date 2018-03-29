package com.futhead.java.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	
	private static final String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:SS";
	private static final String TARGET_FORMAT = "yyyyMMdd";
	
	public static String parseToStr(String dateStr) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(TARGET_FORMAT);
		return sdf.format(getTime(dateStr));
	}
	
	public static Date getTime(String dateStr) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(SOURCE_FORMAT);
		return sdf.parse(dateStr);
	}
	
	public static void main(String[] args) throws ParseException {
		//2018-03-27 10:59:01
		System.out.println(parseToStr("2018-03-27 10:59:01"));
	}
}
