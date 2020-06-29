package me.weiking1021.dbms.finalproject.io.schema;

public abstract class DataSchema {
	
	private int size;
	
	public DataSchema(int size) {
		
		this.size = size;
	}

	public final int getSize() {
		
		return this.size;
	}
	
	public abstract byte[] write(Object object);
	
	public abstract Object read(byte[] bytes);
}