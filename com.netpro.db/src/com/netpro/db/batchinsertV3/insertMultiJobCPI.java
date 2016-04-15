package com.netpro.db.batchinsertV3;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

public class insertMultiJobCPI {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		// TODO Auto-generated method stub
		String jdbcURL="jdbc:postgresql://localhost/trinity4106";
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
			"", //lastupdatetime timestamp without time zone DEFAULT now(),
		};
		
		
	

		try{
			Class.forName("org.postgresql.Driver");
			conn=DriverManager.getConnection(jdbcURL, user, pwd);
			conn.setAutoCommit(false);
			System.out.println("connected");
			
			
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
			}
			    //StringBuffer sb=new StringBuffer("JobUID,JobName, Description,Activate,DomainUID,categoryuid,  agentuid, FrequencyUID, FileSourceUID, JobType, Retry,RetryInterval,MaxRetryTime,RetryMode,Priority,TimeWindowBegin,TimeWindowEnd,TxDateRule,TxOffsetDay,BypassError,status,CriticalJob,createuseruid,XMLData,OnlineDateTime,OfflineDateTime");
			StringBuffer sb=new StringBuffer();
			 Timestamp timestampStart = new Timestamp(System.currentTimeMillis());
			CopyManager cpManager = ((PGConnection)conn).getCopyAPI();
			PushbackReader reader = new PushbackReader( new StringReader(""), 10000 );
			    while(count<=20000){
			    count++;
			   
			    COLUMN_NAMES[1]="job";
				COLUMN_NAMES[0]=UUID.randomUUID().toString();
				//COLUMN_NAMES[0]=UUID.randomUUID().toString().replaceAll("[\\W]|_", "");
				COLUMN_NAMES[1]=COLUMN_NAMES[1]+count;
				//COLUMN_NAMES[2]=dateFormat.format(System.currentTimeMillis());
				insertMultiStepFromJobUid.main(COLUMN_NAMES);  //call jobstep here.
				//insertVSI(COLUMN_NAMES,conn, stmt);  
				insertCPI(COLUMN_NAMES,conn, stmt,count,batchSize,sb,cpManager,reader);  
				
			    }
				reader.unread(sb.toString().toCharArray());
				cpManager.copyIn("COPY JOB FROM STDIN WITH CSV", reader);
				 conn.commit();
				 
				 
				 Timestamp timestampEnd = new Timestamp(System.currentTimeMillis());
				 System.out.println("time elapsed : "+(timestampEnd.getTime() - timestampStart.getTime()));
		}finally{
			if(conn != null)
			{
				System.out.println("[job]connection closed!");
				// clean(conn);
				conn.close();
			}
		}
		
		
	 
	}

	private static void insertCPI(String[] COLUMN_NAMES,
			Connection conn, Statement stmt, Integer count, int batchSize, StringBuffer sb, CopyManager cpManager, PushbackReader reader) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub


        Timestamp timestampStart = new Timestamp(System.currentTimeMillis());
		DateFormat dateFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		//dateFormat.format(System.currentTimeMillis());
		//System.out.println("dateFormat : "+dateFormat.format(System.currentTimeMillis()));
		System.out.println("count in insertCPI :"+count);
	   
		
		//CopyManager cpManager = ((PGConnection)conn).getCopyAPI();
		//PushbackReader reader = new PushbackReader( new StringReader(""), 10000 );
		for(int i=0;i<COLUMN_NAMES.length;i++) {
			  if(i==0){ 
				  sb.append("").append(COLUMN_NAMES[0]).append(",");
				  }
			  else if(i==COLUMN_NAMES.length-1){
				  COLUMN_NAMES[i]=dateFormat.format(System.currentTimeMillis());
					  sb.append(COLUMN_NAMES[i]).append("");
				  }
			  else {  sb.append(COLUMN_NAMES[i]).append(",");
			  }
			  /**
			  if(i==0){ 
				  sb.append("'").append(COLUMN_NAMES[0]).append("','");
				  }
			  else if(i==COLUMN_NAMES.length-1){
					  sb.append(COLUMN_NAMES[i]).append("'");
				  }
			  else {  sb.append(COLUMN_NAMES[i]).append("','");
			  } **/
		}
			sb.append("\n");
	      
	       System.out.println("insert string :  "+sb.toString());
	      if(count % batchSize == 0) {
	    	   try {
	    		//System.out.println("insert string batch :  "+sb.toString());
				reader.unread(sb.toString().toCharArray());
				cpManager.copyIn("COPY JOB FROM STDIN WITH CSV", reader);
				sb.delete(0, sb.length());
			   } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			  }
	      } 
}

	private static void insertVSI(String[] COLUMN_NAMES, Connection conn, Statement stmt) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		StringBuffer sqlbur=new StringBuffer();
		StringBuffer sb=new StringBuffer("JobUID,JobName, Description,Activate,DomainUID,categoryuid,  agentuid, FrequencyUID, FileSourceUID, JobType, Retry,RetryInterval,MaxRetryTime,RetryMode,Priority,TimeWindowBegin,TimeWindowEnd,TxDateRule,TxOffsetDay,BypassError,status,CriticalJob,createuseruid,XMLData,OnlineDateTime,OfflineDateTime,refjobuid");
		 
		 sqlbur.append("INSERT INTO JOB(" + sb.toString() + ")VALUES ('");
	      for(int j=0;j<COLUMN_NAMES.length-1;j++) { 
	    	  sqlbur.append(COLUMN_NAMES[j]+"','");
	      }
	      sqlbur.append(COLUMN_NAMES[COLUMN_NAMES.length-1]+"')");
	      //System.out.println("[job]sqlbur : "+sqlbur.toString());
	      conn.setAutoCommit(true); 
	      int result=stmt.executeUpdate(sqlbur.toString());
	     // System.out.println("[job]result : "+result);
		
	      insertMultiStepFromJobUid.main(COLUMN_NAMES);  //call jobstep here.
	}
	
	 // clean db
    private static void clean(Connection conn) throws SQLException
    {
        conn.createStatement().execute("delete from job");
        conn.commit();
    }
 
}
