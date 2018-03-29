package com.futhead.java.util;

import java.text.ParseException;

import com.futhead.java.model.ClickLog;

public class Test {

//	156.132.29.10	2018-03-27 12:41:01	"GET /www/1 HTTP/1.0"	http://cn.bing.com/search?q=快乐人生	404
	
	public static void main(String[] args) throws NumberFormatException, ParseException {
		String s = "156.132.29.10	2018-03-27 12:41:01	\"GET /www/1 HTTP/1.0\"	http://cn.bing.com/search?q=快乐人生	404";
		String[] infos = s.split("\t");
		System.out.println(infos.length);
        String url = infos[2].split(" ")[1];
        int categrayId = 0;
        if(url.startsWith("/www")) {
        	System.out.println(url);
        	categrayId = Integer.parseInt(url.split("/")[2]);
        }
        new ClickLog(infos[0], DateUtil.parseToStr(infos[1]), categrayId, infos[3], Integer.parseInt(infos[4])).toString();
	}
}
