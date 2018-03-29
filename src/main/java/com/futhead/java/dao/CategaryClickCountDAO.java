package com.futhead.java.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.futhead.java.model.CategaryClickCount;
import com.futhead.java.util.HBaseUtils;

public class CategaryClickCountDAO {

	private static final String tableName = "category_clickcount";
	private static final String cf = "info";
	private static final String qualifer = "click_count";

	
	public static void save(List<CategaryClickCount> categaryClickCounts) throws IOException {
		HTable table = HBaseUtils.getInstance().getHtable(tableName);
		for(CategaryClickCount count: categaryClickCounts) {
			table.incrementColumnValue(Bytes.toBytes(count.getCategaryID()), Bytes.toBytes(cf), Bytes.toBytes(qualifer), count.getClickCout());
		}
	}
	
	public static Long count(String day_categary) throws IOException {
		HTable table = HBaseUtils.getInstance().getHtable(tableName);
		Get get = new Get(Bytes.toBytes(day_categary));
		byte[] value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer));
		if(value == null) {
			return 0L;
		} 
		return Bytes.toLong(value);
	}
	
	public static void main(String[] args) throws IOException {
//		List<CategaryClickCount> list = new ArrayList<>();
//		list.add(new CategaryClickCount("20180327_1", 300L));
//		list.add(new CategaryClickCount("20180327_9", 600L));
//		list.add(new CategaryClickCount("20180327_10", 900L));
//		save(list);
//		System.out.println(count("20180327_1"));
//		System.out.println(count("20180327_9"));
//		System.out.println(count("20180327_10"));
		for (int i=1; i < 7; i++) {
			System.out.println(count("20180329_" + i));
		}
	}
}
