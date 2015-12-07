package com.netpro.db.batchinsert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
		String jdbcURL="jdbc:postgresql://localhost/trinity4030";
		String user="trinity";
		String pwd="trinity";
		Connection conn = null;
		Statement stmt=null;
		ResultSet rs=null;
		final int batchSize = 1000;
		Integer count=0;
		int i,j;
		
		final String[] COLUMN_NAMES = new String[] {
			"",//"jobuid", 0
			"",//"JobName", 1
			"",//"Description", 2
			"1",//"Activate",  3
			"default",//"DomainUID",    4
			"edbd209f-394a-4919-bd5b-98b8ad677ecc", //"categoryuid",  5, need to update for category changed.
			"161a044e-a531-4c4d-b6dd-967ce265d8cb",//"agentuid", 6
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
			
			 
			
			String queryJOB="SELECT * FROM JOB";
			
			rs=stmt.executeQuery(queryJOB);
			while(rs.next())
			{
				 COLUMN_NAMES[1]=rs.getString("JobName");
				 COLUMN_NAMES[5]=rs.getString("CATEGORYUID");
				 COLUMN_NAMES[6]=rs.getString("agentuid");
				 COLUMN_NAMES[23]=rs.getString("xmldata");
				count++;
				System.out.println("[job]Step UID : "+rs.getString("JobUID"));
				System.out.println("[job]Step Seq : "+rs.getString("JobName"));
				System.out.println("[job]Step Name : "+rs.getString("categoryuid"));
			}
			    StringBuffer sb=new StringBuffer("JobUID,JobName, Description,Activate,DomainUID,categoryuid,  agentuid, FrequencyUID, FileSourceUID, JobType, Retry,RetryInterval,MaxRetryTime,RetryMode,Priority,TimeWindowBegin,TimeWindowEnd,TxDateRule,TxOffsetDay,BypassError,status,CriticalJob,createuseruid,XMLData,OnlineDateTime,OfflineDateTime");
			    
			    while(count<=20){
			    //count++;
			    String sqlbur = "";
			   // String sqlbur=new StringBuffer();
			    PreparedStatement ps = conn.prepareStatement(sqlbur);
			    COLUMN_NAMES[1]="job";
				COLUMN_NAMES[0]=UUID.randomUUID().toString();
				//System.out.println("[job]COLUMN_NAMES[0] : "+COLUMN_NAMES[0]);
				COLUMN_NAMES[1]=COLUMN_NAMES[1]+count.toString();
				//System.out.println("[job]COLUMN_NAMES[1] : "+COLUMN_NAMES[1]);
				COLUMN_NAMES[2]=dateFormat.format(System.currentTimeMillis());
				//System.out.println("[job]COLUMN_NAMES[2] : "+COLUMN_NAMES[2]);
			          
				      sqlbur+=("INSERT INTO JOB(" + sb.toString() + ")VALUES ('");
				      //sqlbur.append("INSERT INTO JOB(" + sb.toString() + ")VALUES ('");
				      for(j=0;j<COLUMN_NAMES.length-1;j++) { 
				    	  sqlbur+=(COLUMN_NAMES[j]+"','");
				    	  //sqlbur.append(COLUMN_NAMES[j]+"','");
				      }
				      sqlbur+=(COLUMN_NAMES[COLUMN_NAMES.length-1]+"')");
				      //sqlbur.append(COLUMN_NAMES[COLUMN_NAMES.length-1]+"')");
				     System.out.println("[job]sqlbur : "+sqlbur.toString());
				      ps.addBatch(sqlbur);
				      
				      if(++count % batchSize == 0) {
				    	  conn.setAutoCommit(true); 
				          ps.executeBatch();
				      }
				      
				      //int result=stmt.executeUpdate(sqlbur.toString());
				     // System.out.println("[job]result : "+result);
				      ps.executeBatch(); // insert remaining records
				      ps.close();
				      conn.close();
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
