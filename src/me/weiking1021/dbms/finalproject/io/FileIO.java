package me.weiking1021.dbms.finalproject.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import me.weiking1021.dbms.finalproject.io.schema.DataSchema;

@SuppressWarnings( {"unchecked", "resource"} )
public final class FileIO {
	
	private static final String EXT = ".mydb";
	
	private static final String FOLDER_NAME = "mydbs";
	
	private static final int ROW_LIMIT = 100;
	
	private String table_name;
	
	private DataSchema[] schemas;

	public FileIO(String table_name, Class<? extends DataSchema>... schemas) throws InstantiationException, IllegalAccessException {
		
		this.table_name = table_name;
		this.schemas = new DataSchema[schemas.length];
		
		for (int i=0; i<schemas.length; i++) {

			this.schemas[i] = schemas[i].newInstance();
		}
	}
	
	public File getFile() {
		
		File db_folder = new File(FOLDER_NAME);
		
		return new File(db_folder, this.table_name + EXT);
	}
	
	public boolean exist() {
		
		return this.getFile().exists();
	}
	
	public boolean create() throws IOException {
		
		File file = this.getFile();
		
		if (file.exists()) {
			
			return false;
		}
		
		file.getParentFile().mkdirs();
		
		return file.createNewFile();
	}
	
	public boolean delete() {
		
		return this.getFile().delete();
	}
	
	public boolean copy(String target_table_name) {
 
		FileChannel input;
		FileChannel output;
		
		boolean result = false;
		
		try {
			
			input = new FileInputStream(this.getFile()).getChannel();
			output = new FileOutputStream(new FileIO(target_table_name).getFile()).getChannel();
			
			output.transferFrom(input, 0, input.size());
			
			result = true;
			
			input.close();
			output.close();
		}
		catch (Exception e) {}
		
		return result;
	}
	
	public int schemaByteSizeOffset(int schema_index) {
		
		int offset = 0;
		
		for (int i = 0; i < schema_index; i++) {
			
			offset += this.schemas[i].getSize();
		}
		
		return offset;
	}
	
	public int schemaByteSizeOffset() {
		
		return this.schemaByteSizeOffset(this.schemas.length);
	}
	
	public QueryResult get(boolean ignore_count, SchemaValue... values) throws IOException {
		
		File file = this.getFile();
		
		RandomAccessFile random_file = new RandomAccessFile(file, "r");
		
		FileChannel channel = random_file.getChannel();
		
		int size = this.schemaByteSizeOffset();
		
		QueryResult result = new QueryResult();
		
		for (long index = 0; index < channel.size() / size; index++) {

			channel.position(size * index);
			
			if (values != null && values.length > 0) {
				
				// Filter with specific columns value
				
				if (!this.byteValueFilter(channel, values)) {
					
					continue;
				}
			}

			Object[] row_value = new Object[this.schemas.length];
			
			for (int c = 0; c < this.schemas.length; c++) {
				
				DataSchema schema = this.schemas[c];
				
				ByteBuffer buffer = ByteBuffer.allocate(schema.getSize());
				
				channel.read(buffer);
				
				row_value[c] = schema.read(buffer.array());
			}
			
			result.addRow(row_value);
			
			if (!ignore_count && result.getResult().size() >= ROW_LIMIT) {
				
				break;
			}
		}
		
		random_file.close();
		
		return result;
	}
	
	public QueryResult get(SchemaValue... values) throws IOException {
		
		return this.get(false, values);
	}
	
	public void insert(Object... values) throws IOException {
		
		File file = this.getFile();
		
		RandomAccessFile random_file = new RandomAccessFile(file, "rw");
		
		FileChannel channel = random_file.getChannel();
		
		int size = this.schemaByteSizeOffset();
		
		byte[] inserting_bytes = this.schemas[0].write(values[0]);
		
		long index = 0;
		
		for (; index < channel.size() / size; index++) {

			channel.position(size * index);
				
			DataSchema schema = this.schemas[0];
				
			ByteBuffer buffer = ByteBuffer.allocate(schema.getSize());
				
			channel.read(buffer);
			
			if (this.byteValueComporator(inserting_bytes, buffer.array()) <= 0) {
				
				break;
			}
		}
		
		// Move all after inserting row

		for (long mindex = channel.size() / size; mindex > index; mindex -= 1) {

			ByteBuffer read_buffer = ByteBuffer.allocate(size);
			
			channel.position(size * (mindex - 1));
			
			channel.read(read_buffer);
			
			ByteBuffer write_buffer = ByteBuffer.wrap(read_buffer.array());
			
			channel.position(size * mindex);
			
			channel.write(write_buffer);
		}
		
		channel.position(size * index);
		
		for (int c = 0; c < this.schemas.length; c++) {
			
			DataSchema schema = this.schemas[c];
			
			ByteBuffer buffer = ByteBuffer.wrap(schema.write(values[c]));
			
			channel.write(buffer);
		}
		
		random_file.close();
	}
	
	public void append(Object... values) throws IOException {
		
		File file = this.getFile();
		
		RandomAccessFile random_file = new RandomAccessFile(file, "rw");
		
		FileChannel channel = random_file.getChannel();
		
		channel.position(channel.size());
		
		for (int c = 0; c < this.schemas.length; c++) {
			
			DataSchema schema = this.schemas[c];
			
			ByteBuffer buffer = ByteBuffer.wrap(schema.write(values[c]));
			
			channel.write(buffer);
		}
		
		random_file.close();
	}
	
	public void delete(SchemaValue... values) throws IOException {
		
		File file = this.getFile();
		
		// Just clear all contents with writer
		if (values == null || values.length == 0) {
			
			new PrintWriter(file).close();
		}
		
		RandomAccessFile random_file = new RandomAccessFile(file, "rw");
		
		FileChannel channel = random_file.getChannel();
		
		int size = this.schemaByteSizeOffset();

		for (long index = 0; index < channel.size() / size; index++) {

			channel.position(size * index);
			
			if (!this.byteValueFilter(channel, values)) {
				
				continue;
			}
			
			channel.position(size * index);
			
			// Move all after inserting row
			long mindex = index;
			
			for (; mindex < channel.size() / size; mindex += 1) {

				ByteBuffer read_buffer = ByteBuffer.allocate(size);
				
				channel.position(size * (mindex + 1));
				
				channel.read(read_buffer);
				
				ByteBuffer write_buffer = ByteBuffer.wrap(read_buffer.array());
				
				channel.position(size * mindex);
				
				channel.write(write_buffer);
			}
			
			channel.truncate(channel.size() - size);
		}
		
		random_file.close();
	}
	
	private boolean byteValueFilter(FileChannel channel, SchemaValue... values) throws IOException {
		
		long position = channel.position();

		for (SchemaValue value : values) {

			int index = value.getIndex();
			
			DataSchema schema = this.schemas[index];
			
			int offset = this.schemaByteSizeOffset(index);
			
			channel.position(position + offset);

			ByteBuffer buffer = ByteBuffer.allocate(schema.getSize());
			
			channel.read(buffer);

			byte[] target_bytes = buffer.array();

			byte[] cond_bytes = schema.write(value.getValue());
			
			if (!Arrays.equals(target_bytes, cond_bytes)) {
				
				channel.position(position);
				
				return false;
			}
		}
		
		channel.position(position);
		
		return true;
	}
	
	private int byteValueComporator(byte[] a, byte[] b) {
		
		if (a.length != b.length) {
			
			return 0;
		}
		
		int length = a.length;
		
		for (int i=0; i<length; i++) {
			
			int uia = Byte.toUnsignedInt(a[i]);
			int uib = Byte.toUnsignedInt(b[i]);
			
			int result = Integer.compare(uia, uib);
			
			if (result != 0) {
				
				return result;
			}
		}
		
		return 0;
	}
}
