package me.weiking1021.dbms.finalproject.io.schema;

import java.nio.ByteBuffer;

public class IntegerSchema extends DataSchema {

	public IntegerSchema() {
		
		super(4);
	}

	@Override
	public final byte[] write(Object object) {
		
		byte[] bytes = new byte[this.getSize()];

		if (!(object instanceof Integer)) {
			
			return bytes;
		}

		return ByteBuffer.allocate(this.getSize()).putInt((int) object).array();
	}

	@Override
	public final Integer read(byte[] bytes) {
		
		return ByteBuffer.wrap(bytes).getInt();
	}

}
