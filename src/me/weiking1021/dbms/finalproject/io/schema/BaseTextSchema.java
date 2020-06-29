package me.weiking1021.dbms.finalproject.io.schema;

import java.nio.charset.Charset;

public abstract class BaseTextSchema extends DataSchema {

	public BaseTextSchema(int text_size) {
		
		super(text_size);
	}

	@Override
	public final byte[] write(Object object) {
		
		byte[] bytes = new byte[this.getSize()];

		if (!(object instanceof String)) {
			
			return bytes;
		}

		String text = (String) object;
		
		byte[] text_bytes = text.getBytes();
		
		for (int i=0; i<text.length(); i++) {
			
			//bytes[bytes.length - text.length() + i] = text_bytes[i];
			bytes[i] = text_bytes[i];
		}
		
		return bytes;
	}

	@Override
	public final String read(byte[] bytes) {
		
		return new String(bytes, Charset.forName("UTF-8"));
	}

}
