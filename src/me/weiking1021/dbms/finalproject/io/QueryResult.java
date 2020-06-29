package me.weiking1021.dbms.finalproject.io;

import java.util.ArrayList;
import java.util.List;

public class QueryResult {

	private List<Object[]> result_list;
	
	public QueryResult() {
		
		this.result_list = new ArrayList<>();
	}
	
	public void addRow(Object[] values) {
		
		this.result_list.add(values);
	}
	
	public List<Object[]> getResult() {
		
		return this.result_list;
	}
}
