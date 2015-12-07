package com.netpro.db.batchinsert;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.UUID;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

public class batchInsertMultiJob {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		String jdbcURL="jdbc:postgresql://localhost/trinity0430";  //need to config
		String user="trinity"; //need to config
		String pwd="trinity";  //need to config
		Connection conn = null;
		Statement stmt=null;
		ResultSet rs=null;
		final int batchSize = 3;
		Integer count = 0;
		final String[] COLUMN_NAMES = new String[] {
			"",//"jobuid", 0
			"",//"JobName", 1
			"",//"Description", 2
			"1",//"Activate",  3
			"default",//"DomainUID",    4
			"", //"categoryuid",  5, need to update for category changed.
			"bbbc94da-bc6d-4c7d-860c-237e33947c1b",//"agentuid", 6
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
			"",//refjobuid,26, new in 4.0.30
			"",//lastupdatetime,27, new in 4.0.30
		};
		
		
	

		try{
			Class.forName("org.postgresql.Driver");
			conn=DriverManager.getConnection(jdbcURL, user, pwd);
			conn.setAutoCommit(false);
			System.out.println("postgre connected");
			
			DateFormat dateFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			dateFormat.format(System.currentTimeMillis());
			//System.out.println("dateFormat : "+dateFormat.format(System.currentTimeMillis()));
			//Date date=new Date();
			//System.out.println("Date : "+date);
			
			//Creating statement for db activities
			stmt=conn.createStatement();
			
			//select jobcategory uid
			String catName="CATBATCH";
			String queryCatName="SELECT categoryname,categoryuid FROM JOBCATEGORY";
			rs=stmt.executeQuery(queryCatName);
			while(rs.next()) {
				if(rs.getString("categoryname").equalsIgnoreCase(catName)) {
				 System.out.println("CATEGORYNAME : "+rs.getString("categoryname"));
				COLUMN_NAMES[5]=rs.getString("categoryuid"); }
				
			}
			
			
			String query3="SELECT categoryuid,xmldata  FROM JOB";
		 
			rs=stmt.executeQuery(query3);
			while(rs.next())
			{
				if(rs.getString("categoryuid").equalsIgnoreCase(COLUMN_NAMES[5])){
				 //COLUMN_NAMES[1]=rs.getString("JobName");
				 COLUMN_NAMES[23]=rs.getString("xmldata");
				System.out.println("[job]Step Seq : "+rs.getString("xmldata"));
				System.out.println("[job]Step Name : "+rs.getString("categoryuid"));
				count++;
				}
			}
			
			StringBuilder sb = new StringBuilder();
			CopyManager cpManager = ((PGConnection)conn).getCopyAPI();
			PushbackReader reader = new PushbackReader( new StringReader(""), 10000 );
			  //  StringBuffer sb=new StringBuffer("JobUID,JobName, Description,Activate,DomainUID,categoryuid,  agentuid, FrequencyUID, FileSourceUID, JobType, Retry,RetryInterval,MaxRetryTime,RetryMode,Priority,TimeWindowBegin,TimeWindowEnd,TxDateRule,TxOffsetDay,BypassError,status,CriticalJob,createuseruid,XMLData,OnlineDateTime,OfflineDateTime");
			    
		 
				while(count<=30){
				count++;
			    COLUMN_NAMES[1]="job";
				COLUMN_NAMES[0]=UUID.randomUUID().toString().replaceAll("[\\W]|_", "");
				System.out.println("jobuid : "+COLUMN_NAMES[0]);
				COLUMN_NAMES[1]=COLUMN_NAMES[1]+count.toString();
				System.out.println("jobname : "+COLUMN_NAMES[1]);
				//COLUMN_NAMES[2]=dateFormat.format(System.currentTimeMillis());
				System.out.println("time : "+COLUMN_NAMES[2]);
				System.out.println("COLUMN_NAMES.length : "+COLUMN_NAMES.length);
			          
				      
				    //the below loop is for ordering the value of row of table job  
					for(int i=0;i<COLUMN_NAMES.length-1;i++) {
						  if(i==0){ 
							  sb.append("'").append(COLUMN_NAMES[0]).append("','");
							  } else if(i==COLUMN_NAMES.length-2){
								  sb.append(COLUMN_NAMES[i]).append("'");
							  }
						  else {  sb.append(COLUMN_NAMES[i]).append("','");
						  }
					}
						sb.append("\n");
				      
				       System.out.println("insert string :  "+sb.toString());
				      
				      if(count % batchSize == 0) {
				    	   try {
							reader.unread(sb.toString().toCharArray());
							cpManager.copyIn("COPY JOB FROM STDIN WITH CSV", reader);
							sb.delete(0, sb.length());
						   } catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						  }
				      }
				       
				 	
				} 
				try {
					reader.unread(sb.toString().toCharArray());
					cpManager.copyIn("COPY JOB FROM STDIN WITH CSV", reader);   
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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
