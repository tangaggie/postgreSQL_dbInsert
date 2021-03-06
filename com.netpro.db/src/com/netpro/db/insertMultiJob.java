package com.netpro.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class insertMultiJob {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		String jdbcURL="jdbc:postgresql://localhost/trinity0430";
		String user="trinity";
		String pwd="trinity";
		Connection conn = null;
		Statement stmt=null;
		ResultSet rs=null;
		Integer count=0;
		int i,j;
		
		final String[] COLUMN_NAMES = new String[] {
			"",//"jobuid", 0
			"",//"JobName", 1
			"",//"Description", 2
			"1",//"Activate",  3
			"default",//"DomainUID",    4
			"", //"categoryuid",  5, need to update for category changed.
			"",//"agentuid", 6
			"",//"FrequencyUID", 7
			"",//"FileSourceUID", 8
			"1",//"JobType", 9
			"0",//"Retry",10
			"1",//"RetryInterval",11
			"1",//"MaxRetryTime",12
	        "0",//"RetryMode",13
			"1",//"Priority",14
			"1",//"TimeWindowBegin",15
			"1439",//"TimeWindowEnd",16
			"0",//"TxDateRule",17
			"0",//"TxOffsetDay",18
			"1",//"BypassError",19
			"O",//"status",20
			"0",//"CriticalJob",21
			"trinity",//"createuseruid",22
			"",//"XMLData",23
			"",//"OnlineDateTime",24
			"",//"OfflineDateTime",25
			"",//refjobuid character varying(36),26
		};
		
		
	

		try{
			Class.forName("org.postgresql.Driver");
			conn=DriverManager.getConnection(jdbcURL, user, pwd);
			conn.setAutoCommit(false);
			System.out.println("connected");
			
			DateFormat dateFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			//dateFormat.format(System.currentTimeMillis());
			System.out.println("dateFormat : "+dateFormat.format(System.currentTimeMillis()));
			Date date=new Date();
			System.out.println("Date : "+date);
			
			//Creating statement for db activities
			stmt=conn.createStatement();
			
			//select jobcategory uid
			String query1="SELECT * FROM JOBCATEGORY";
			rs=stmt.executeQuery(query1);
			int rscount=0;
			while(rs.next()) {	
				rscount++;
				COLUMN_NAMES[5]=rs.getString("categoryuid");
				System.out.println("[job]CATEGORYUID : "+rs.getString("categoryuid")+"number : "+rscount);
			}
			
			//select AGENT uid
			String query2="SELECT * FROM JCSAGENT";
			rs=stmt.executeQuery(query2);
			while(rs.next()) COLUMN_NAMES[6]=rs.getString("agentuid");
			
			String query3="SELECT * FROM JOB";
			
			rs=stmt.executeQuery(query3);
			while(rs.next())
			{
				 COLUMN_NAMES[1]=rs.getString("JobName");
				 COLUMN_NAMES[23]=rs.getString("xmldata");
				count++;
				System.out.println("[job]Step UID : "+rs.getString("JobUID"));
				System.out.println("[job]Step Seq : "+rs.getString("JobName"));
				System.out.println("[job]Step Name : "+rs.getString("categoryuid"));
			}
			    StringBuffer sb=new StringBuffer("JobUID,JobName, Description,Activate,DomainUID,categoryuid,  agentuid, FrequencyUID, FileSourceUID, JobType, Retry,RetryInterval,MaxRetryTime,RetryMode,Priority,TimeWindowBegin,TimeWindowEnd,TxDateRule,TxOffsetDay,BypassError,status,CriticalJob,createuseruid,XMLData,OnlineDateTime,OfflineDateTime");
			    
			    while(count<=20){
			    count++;
			    StringBuffer sqlbur=new StringBuffer();
			    COLUMN_NAMES[1]="job";
				COLUMN_NAMES[0]=UUID.randomUUID().toString();
				//System.out.println("[job]COLUMN_NAMES[0] : "+COLUMN_NAMES[0]);
				COLUMN_NAMES[1]=COLUMN_NAMES[1]+count.toString();
				//System.out.println("[job]COLUMN_NAMES[1] : "+COLUMN_NAMES[1]);
				COLUMN_NAMES[2]=dateFormat.format(System.currentTimeMillis());
				//System.out.println("[job]COLUMN_NAMES[2] : "+COLUMN_NAMES[2]);
			
				      sqlbur.append("INSERT INTO JOB(" + sb.toString() + ")VALUES ('");
				      for(j=0;j<COLUMN_NAMES.length-1;j++) { 
				    	  sqlbur.append(COLUMN_NAMES[j]+"','");
				      }
				      sqlbur.append(COLUMN_NAMES[COLUMN_NAMES.length-1]+"')");
				     // System.out.println("[job]sqlbur : "+sqlbur.toString());
				      conn.setAutoCommit(true); 
				      int result=stmt.executeUpdate(sqlbur.toString());
				     // System.out.println("[job]result : "+result);
				      
				      insertMultiStepFromJobUid.main(COLUMN_NAMES);  //call jobstep here.
			    }
		}finally{
			if(conn != null)
			{
				System.out.println("[job]connection closed!");
				conn.close();
			}
		}
		
		

	}

}
