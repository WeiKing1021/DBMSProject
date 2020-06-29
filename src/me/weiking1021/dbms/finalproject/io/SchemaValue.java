package me.weiking1021.dbms.finalproject.io;

public final class SchemaValue {

	private int index;
	
	private Object value;
	
	public SchemaValue(int index, Object value) {
		
		this.index = index;
		this.value = value;
	}
	
	public int getIndex() {
		
		return this.index;
	}
	
	public Object getValue() {
		
		return this.value;
	}
}
