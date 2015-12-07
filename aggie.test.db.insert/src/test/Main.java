package test;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.util.PSQLException;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;

import static java.lang.System.*;

public class Main
{
    static final String connUrl = "jdbc:postgresql://localhost:5432/testaggie";
    static final String user = "postgres";
    static final String password = "trinity";

    static final long measurementId = 1L;
    static final BigDecimal measurementValue = new BigDecimal("99999999.7777");

    static final int testSize = 1000;
    static long[] measurementIds;
    static Timestamp[] timestamps;
    static BigDecimal[] values;

    public static void main (String args[]) throws ClassNotFoundException, SQLException, InterruptedException, IOException {
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

       out.println("index, VSI, SPI, BPI-10, BPI-20, BPI-50, BPI-100, BPI-200, CPI-10, CPI-20, CPI-50, CPI-100, CPI-200");

       for (int i=1; i<=iterations; i++)
        {
           out.print(i); out.print(",");
           out.print(runVSI(conn)); out.print(","); Thread.sleep(sleep);
           out.print(runSPI(conn)); out.print(","); Thread.sleep(sleep);

           out.print(runBPI(conn,10)); out.print(","); Thread.sleep(sleep);
           out.print(runBPI(conn,20)); out.print(","); Thread.sleep(sleep);
           out.print(runBPI(conn,50)); out.print(","); Thread.sleep(sleep);
           out.print(runBPI(conn,100)); out.print(","); Thread.sleep(sleep);
           out.print(runBPI(conn,200)); out.print(","); Thread.sleep(sleep);

           out.print(runCPI(conn, 10)); out.print(","); Thread.sleep(sleep);
           out.print(runCPI(conn, 20)); out.print(","); Thread.sleep(sleep);
           out.print(runCPI(conn, 50)); out.print(","); Thread.sleep(sleep);
           out.print(runCPI(conn, 100)); out.print(","); Thread.sleep(sleep);
           out.print(runCPI(conn, 200)); Thread.sleep(sleep);
           out.println();
       }

       conn.close();
    }

    // Very Stupid Inserts
    private static long runVSI(Connection conn) throws SQLException
    {
        clean(conn);

        Statement insert = conn.createStatement();
        String insertSQL = "";
        Timestamp timestampStart = new Timestamp(System.currentTimeMillis());
        // store data - begin
        for (int i=0; i<testSize; i++)
        {
            insertSQL = "insert into mv_raw values (" + measurementIds[i] +",'"+ timestamps[i] +"',"+values[i]+")";
            insert.execute(insertSQL);
        }
        conn.commit();
        // store data - end

        Timestamp timestampEnd = new Timestamp(System.currentTimeMillis());
        long result = timestampEnd.getTime() - timestampStart.getTime();

        clean(conn);

        return timestampEnd.getTime() - timestampStart.getTime();
    }

    // Stupid Prepared Inserts
    private static long runSPI(Connection conn) throws SQLException
    {
        clean(conn);

        PreparedStatement insert = conn.prepareStatement("insert into mv_raw values (?,?,?)");

        Timestamp timestampStart = new Timestamp(System.currentTimeMillis());
        // store data - begin
        for (int i=0; i<testSize; i++)
        {
            insert.setLong(1,measurementIds[i]);
            insert.setTimestamp(2, timestamps[i]);
            insert.setBigDecimal(3, values[i]);
            insert.execute();
        }
        conn.commit();
        // store data - end

        Timestamp timestampEnd = new Timestamp(System.currentTimeMillis());
        long result = timestampEnd.getTime() - timestampStart.getTime();

        clean(conn);

        return result;
    }

    // Batched Prepared Inserts
    private static long runBPI(Connection conn, int batchSize) throws SQLException
    {
        clean(conn);

        PreparedStatement insert = conn.prepareStatement("insert into mv_raw values (?,?,?)");

        Timestamp timestampStart = new Timestamp(System.currentTimeMillis());
        // store data - begin
        for (int i=0; i<testSize; i++)
        {
            insert.setLong(1,measurementIds[i]);
            insert.setTimestamp(2, timestamps[i]);
            insert.setBigDecimal(3, values[i]);
            insert.addBatch();
            if (i % batchSize == 0) { insert.executeBatch(); }
        }
        insert.executeBatch();
        conn.commit();
        // store data - end

        Timestamp timestampEnd = new Timestamp(System.currentTimeMillis());
        long result = timestampEnd.getTime() - timestampStart.getTime();

        clean(conn);

        return result;
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

        clean(conn);

        return result;
    }

    // clean db
    private static void clean(Connection conn) throws SQLException
    {
        conn.createStatement().execute("delete from mv_raw");
        conn.commit();
    }
}
