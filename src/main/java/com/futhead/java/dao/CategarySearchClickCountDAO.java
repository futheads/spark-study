package com.futhead.java.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.futhead.java.model.CategarySearchClickCount;
import com.futhead.java.util.HBaseUtils;

public class CategarySearchClickCountDAO {

	private static final String tableName = "categary_search_cout";
	private static final String cf = "info";
	private static final String qualifer = "click_count";
	
	public static void save(List<CategarySearchClickCount> categaryClickCounts) throws IOException {
		HTable table = HBaseUtils.getInstance().getHtable(tableName);
		for(CategarySearchClickCount count: categaryClickCounts) {
			table.incrementColumnValue(Bytes.toBytes(count.getDaySearchCategary()), Bytes.toBytes(cf), Bytes.toBytes(qualifer), count.getClickCout());
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
//		List<CategarySearchClickCount> list = new ArrayList<>();
//		list.add(new CategarySearchClickCount("20171122_1_1", 300L));
//		list.add(new CategarySearchClickCount("20171122_1_2", 600L));
//		list.add(new CategarySearchClickCount("20171122_1_3", 900L));
//		save(list);
		for (int i=1; i < 4; i++) {
			System.out.println(count("20180328_www.sogou.com_" + i));
		}
	}
	
}
