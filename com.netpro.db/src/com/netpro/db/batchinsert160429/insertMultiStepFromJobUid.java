package com.netpro.db.batchinsert160429;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class insertMultiStepFromJobUid {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		String jdbcURL="jdbc:postgresql:////localhost/trinity4106";
		String user="trinity";
		String pwd="trinity";
		Connection conn = null;
		Statement stmt=null;
		ResultSet rs=null;
		String uidtmp = null;
		
		final String[] COLUMN_NAMES = new String[] {
			"",//"StepUID", 0
			"",//"StepSeq", 1
			"",//"StepName", 2
			"",//"Description",  3
			"1",//"Activate",    4
			"", //"JobUID",  5
			"A",//"StepType", 6
			"1",//"SuccessRule", 7
			"0",//"SuccessValue1", 8
			"0",//"SuccessValue2", 9
			"",//"XMLData" 10
		};

		try{
			Class.forName("org.postgresql.Driver");
			conn=DriverManager.getConnection(jdbcURL, user, pwd);
			conn.setAutoCommit(false);
			System.out.println("connected");
			
			//Creating statement for db activities
			stmt=conn.createStatement();
			String query="SELECT distinct StepUID FROM JOBSTEP";
			
			rs=stmt.executeQuery(query);
		    while(rs.next())
			{
			 	 uidtmp=rs.getString("StepUID");
				 
			 
			}
			
			StringBuffer sb=new StringBuffer("StepUID,StepSeq,StepName,Description,Activate,JobUID,StepType,SuccessRule,SuccessValue1,SuccessValue2,XMLData");
			    
			 StringBuffer sqlbur=new StringBuffer();
				 
		      sqlbur.append("INSERT INTO JOBSTEP(" + sb.toString() + ")");
	          sqlbur.append("SELECT '"+UUID.randomUUID()+"',StepSeq,StepName,Description,Activate,'"+args[0]+"',StepType,SuccessRule,SuccessValue1,SuccessValue2,XMLData");
	          sqlbur.append("FROM JOBSTEP");
	          sqlbur.append("WHERE stepUID="+uidtmp+", jobuid ="+args[0]+";");
				      
				      conn.setAutoCommit(true); 
				      int result=stmt.executeUpdate(sqlbur.toString());
				  //    System.out.println("result : "+result);
			    
		}finally{
			if(conn != null)
			{
				System.out.println("connection closed!");
				conn.close();
			}
		}
		
		

	}

}
