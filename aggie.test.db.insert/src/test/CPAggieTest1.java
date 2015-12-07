package test;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.util.PSQLException;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;

import static java.lang.System.*;

public class CPAggieTest1 {
	static final String connUrl = "jdbc:postgresql://localhost:5432/testaggie";
    static final String user = "postgres";
    static final String password = "trinity";

    static final long measurementId = 1L;
    static final BigDecimal measurementValue = new BigDecimal("99999999.7777");

    static final int testSize = 1000;
    static long[] measurementIds;
    static Timestamp[] timestamps;
    static BigDecimal[] values;

	public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException, IOException {
		// TODO Auto-generated method stub
		  Class.forName("org.postgresql.Driver"); //load the driver
	       Connection conn = DriverManager.getConnection(connUrl, user, password);

	       // generate data - begin
	       measurementIds = new long[testSize];
	       timestamps = new Timestamp[testSize];
	       values = new BigDecimal[testSize];
	       long initTime = System.currentTimeMillis();
	       for (int i=0; i<testSize; i++)
	       {
	          measurementIds[i] = measurementId; // growing
	          values[i] = measurementValue; // const
	          timestamps[i] = new Timestamp(initTime+(i*1000));
	       }
	       // generate data - end

	       conn.setAutoCommit(false);

	       int sleep = 500;
	       int iterations = 200;

	       out.println("index, CPI-10, CPI-20, CPI-50, CPI-100, CPI-200");

	       for (int i=1; i<=iterations; i++)
	        {
	    	   out.print(i); out.print(",");
	    	   out.print(runCPI(conn, 10)); out.print(","); Thread.sleep(sleep);
	           out.print(runCPI(conn, 20)); out.print(","); Thread.sleep(sleep);
	           out.print(runCPI(conn, 50)); out.print(","); Thread.sleep(sleep);
	           out.print(runCPI(conn, 100)); out.print(","); Thread.sleep(sleep);
	           out.print(runCPI(conn, 200)); Thread.sleep(sleep);
	           out.println();
	        }
	       conn.close();
	}
	// COPY Inserts
    private static long runCPI(Connection conn, int batchSize) throws SQLException, IOException
    {
        clean(conn);

        Timestamp timestampStart = new Timestamp(System.currentTimeMillis());
        // store data - begin
        StringBuilder sb = new StringBuilder();
        CopyManager cpManager = ((PGConnection)conn).getCopyAPI();
        PushbackReader reader = new PushbackReader( new StringReader(""), 10000 );
        for (int i=0; i<testSize; i++)
        {
            sb.append(measurementIds[i]).append(",'").append(timestamps[i]).append("',").append(values[i]).append("\n");

            if (i % batchSize == 0)
            {
                //cpManager.copyIn("COPY mv_raw FROM STDIN WITH CSV", new StringReader(sb.toString()) );
                reader.unread( sb.toString().toCharArray() );
                cpManager.copyIn("COPY mv_raw FROM STDIN WITH CSV", reader );
                sb.delete(0,sb.length());
            }
        }
        // cpManager.copyIn("COPY mv_raw FROM STDIN WITH CSV", new StringReader(sb.toString()) );
        reader.unread( sb.toString().toCharArray() );
        cpManager.copyIn("COPY mv_raw FROM STDIN WITH CSV", reader );
        conn.commit();
        // store data - end

        Timestamp timestampEnd = new Timestamp(System.currentTimeMillis());
        long result = timestampEnd.getTime() - timestampStart.getTime();

        //clean(conn);

        return result;
    }

    // clean db
    private static void clean(Connection conn) throws SQLException
    {
        conn.createStatement().execute("delete from mv_raw");
        conn.commit();
    }
}
