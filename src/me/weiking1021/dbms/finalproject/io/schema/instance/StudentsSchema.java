package me.weiking1021.dbms.finalproject.io.schema.instance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import me.weiking1021.dbms.finalproject.io.FileIO;
import me.weiking1021.dbms.finalproject.io.QueryResult;
import me.weiking1021.dbms.finalproject.io.SchemaValue;
import me.weiking1021.dbms.finalproject.io.schema.Byte16TextSchema;
import me.weiking1021.dbms.finalproject.io.schema.Byte4TextSchema;

@SuppressWarnings("unchecked")
public class StudentsSchema {
	
	private FileIO io;
	
	public StudentsSchema() throws Exception {
		
		this.io = new FileIO("students", Byte16TextSchema.class, Byte4TextSchema.class);
	}
	
	public QueryResult findByID(String id) throws IOException {
		
		return this.io.get(new SchemaValue(0, id));
	}
	
	public List<String> findListByID(String id) throws IOException {
		
		QueryResult result = this.findByID(id);
		
		List<String> result_list = new ArrayList<>();
		
		for (Object[] objects : result.getResult()) {
			
			result_list.add((String) objects[1]);
		}
		
		return result_list;
	}
	
	public int courseStudentCount(String course_id) throws IOException {
		
		QueryResult result = this.io.get(true, new SchemaValue(1, course_id));
		
		return result.getResult().size();
	}
	
	public void insert(String student_id, String course_id) throws IOException {
		
		this.io.append(student_id, course_id);
	}
	
	public void delete(String student_id, String course_id) throws IOException {
		
		this.io.delete(new SchemaValue(0, student_id), new SchemaValue(1, course_id));
	}
	
	public QueryResult readWithLimit() throws IOException {
		
		return this.io.get();
	}
}
