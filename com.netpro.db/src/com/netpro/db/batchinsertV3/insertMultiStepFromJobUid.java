package com.netpro.db.batchinsertV3;

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
		String jdbcURL="jdbc:postgresql://localhost/trinity";
		String user="trinity";
		String pwd="trinity";
		Connection conn = null;
		Statement stmt=null;
		ResultSet rs=null;
		Integer count=0;
		int i,j;
		
		final String[] COLUMN_NAMES = new String[] {
			"",//"StepUID", 0
			"",//"StepSeq", 1
			"",//"StepName", 2
			"",//"Description",  3
			"1",//"Activate",    4
			"c964831e-f750-4ff6-9150-3553946a3e8e", //"JobUID",  5
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
			
			//DateFormat dateFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			//Date date=new Date();
			
			//Creating statement for db activities
			stmt=conn.createStatement();
			String query="SELECT * FROM JOBSTEP";
			
			rs=stmt.executeQuery(query);
		    while(rs.next())
			{
			 //	 COLUMN_NAMES[5]=rs.getString("JobUID");
				 COLUMN_NAMES[10]=rs.getString("XMLData");
			//	count++;
				System.out.println("Step UID : "+rs.getString("stepUID"));
				System.out.println("Step Seq : "+rs.getString("stepseq"));
				System.out.println("Step Name : "+rs.getString("stepname"));
			}
			    StringBuffer sb=new StringBuffer("StepUID,StepSeq,StepName,Description,Activate,JobUID,StepType,SuccessRule,SuccessValue1,SuccessValue2,XMLData");
			    
			    for(i=0;i<3;i++){
			    	count++;
			    StringBuffer sqlbur=new StringBuffer();
				COLUMN_NAMES[0]=UUID.randomUUID().toString();
				System.out.println("StepUID : "+COLUMN_NAMES[0]);
				COLUMN_NAMES[1]=count.toString();
				System.out.println("StepSeq : "+COLUMN_NAMES[1]);
				COLUMN_NAMES[2]="STEP"+count.toString();
				System.out.println("StepName : "+COLUMN_NAMES[2]);
				COLUMN_NAMES[5]=args[0];
				System.out.println("JobUID : "+COLUMN_NAMES[5]);
			
				      sqlbur.append("INSERT INTO JOBSTEP(" + sb.toString() + ")VALUES ('");
				      for(j=0;j<COLUMN_NAMES.length-1;j++) { 
				    	  sqlbur.append(COLUMN_NAMES[j]+"','");
				      }
				      sqlbur.append(COLUMN_NAMES[COLUMN_NAMES.length-1]+"')");
				      System.out.println("sqlbur : "+sqlbur.toString());
				      conn.setAutoCommit(true); 
				      int result=stmt.executeUpdate(sqlbur.toString());
				      System.out.println("result : "+result);
			    }
		}finally{
			if(conn != null)
			{
				System.out.println("connection closed!");
				conn.close();
			}
		}
		
		

	}

}
