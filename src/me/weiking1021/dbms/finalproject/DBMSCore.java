package me.weiking1021.dbms.finalproject;

import me.weiking1021.dbms.finalproject.io.QueryResult;
import me.weiking1021.dbms.finalproject.io.schema.instance.StudentsSchema;

public class DBMSCore {

	public static void main(String[] args) throws Exception {
		
		StudentsSchema schema = new StudentsSchema();
		
		// 找指定學生的課程並顯示
		String student_id = "D0596498";
		QueryResult result = schema.findByID(student_id);
		
		System.out.println("==============================");
		System.out.println("學生(" + student_id + ")的已選課程：");
		for (Object[] objects : result.getResult()) {
			
			for (int i=0; i<objects.length; i++) {
				
				if (i != 0) {
					
					System.out.print(" - ");
				}
				
				System.out.print(objects[i]);
			}
			
			System.out.println();
		}
		
		// 找指定課程的已修人數
		String course_id = "2148";
		
		System.out.println("==============================");
		System.out.println("課程(" + course_id + ")的總修習人數：");
		int count = schema.courseStudentCount(course_id);
		
		System.out.println(count + " 人");
		
		// 幫該學生加選一堂代碼為9999的課
		schema.insert(student_id, "9999");
		result = schema.findByID(student_id);
		System.out.println("==============================");
		System.out.println("**幫該學生加選一堂代碼為9999的課**");
		System.out.println("學生(" + student_id + ")的已選課程：");
		for (Object[] objects : result.getResult()) {
			
			for (int i=0; i<objects.length; i++) {
				
				if (i != 0) {
					
					System.out.print(" - ");
				}
				
				System.out.print(objects[i]);
			}
			
			System.out.println();
		}
		
		// 幫該學生退選一堂代碼為9999的課
		schema.delete(student_id, "9999");
		result = schema.findByID(student_id);
		System.out.println("==============================");
		System.out.println("**幫該學生退選一堂代碼為9999的課**");
		System.out.println("學生(" + student_id + ")的已選課程：");
		for (Object[] objects : result.getResult()) {
			
			for (int i=0; i<objects.length; i++) {
				
				if (i != 0) {
					
					System.out.print(" - ");
				}
				
				System.out.print(objects[i]);
			}
			
			System.out.println();
		}
		
		// 全部顯示 ((只有100筆資料
		result = schema.readWithLimit();
		System.out.println("==============================");
		
		int index = 0;
		
		for (Object[] objects : result.getResult()) {
			System.out.print(++index + ". ");
			for (int i=0; i<objects.length; i++) {
				
				if (i != 0) {
					
					System.out.print(" - ");
				}
				
				System.out.print(objects[i]);
			}
			
			System.out.println();
		}
	}
}
