package org.kendar.janus;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.kendar.janus.utils.TestBase;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class PSTatementGetSetTest extends TestBase {

    @BeforeEach
    protected void beforeEach(){
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }


    // org.h2.jdbc.JdbcSQLFeatureNotSupportedException: Feature not supported: "url" [50100-214]
    //  Arguments.of(new Class<?>[]{URL.class}, "VARCHAR(255)",new Object[]{new URL("http://www.google.com")},"URL")
    private static Stream<Arguments> provideTestStatements() throws MalformedURLException {
        return Stream.of(
                Arguments.of(new Class<?>[]{int.class}, "INTEGER",new Object[]{22},"Int"),
                Arguments.of(new Class<?>[]{String.class}, "VARCHAR(255)",new Object[]{"TEST"},"String"),
                Arguments.of(new Class<?>[]{Date.class}, "DATE",new Object[]{new Date(123)},"Date"),
                Arguments.of(new Class<?>[]{Time.class}, "TIME",new Object[]{new Time(123)},"Time"),
                Arguments.of(new Class<?>[]{Timestamp.class}, "TIMESTAMP",new Object[]{new Timestamp(123)},"Timestamp"),
                Arguments.of(new Class<?>[]{byte.class}, "TINYINT",new Object[]{(byte)44},"Byte"),
                Arguments.of(new Class<?>[]{double.class}, "DOUBLE PRECISION",new Object[]{0.55},"Double"),
                Arguments.of(new Class<?>[]{float.class}, "FLOAT",new Object[]{0.55F},"Float"),
                Arguments.of(new Class<?>[]{short.class}, "INTEGER",new Object[]{(short)22},"Short"),
                Arguments.of(new Class<?>[]{boolean.class}, "TINYINT",new Object[]{true},"Boolean"),
                Arguments.of(new Class<?>[]{BigDecimal.class}, "TINYINT",new Object[]{BigDecimal.valueOf(77)},"BigDecimal"),
                Arguments.of(new Class<?>[]{BigDecimal.class}, "DOUBLE PRECISION",new Object[]{BigDecimal.valueOf(77)},"BigDecimal"),
                Arguments.of(new Class<?>[]{BigDecimal.class}, "FLOAT",new Object[]{BigDecimal.valueOf(77)},"BigDecimal"),
                Arguments.of(new Class<?>[]{BigDecimal.class}, "INTEGER",new Object[]{BigDecimal.valueOf(77)},"BigDecimal")

                ,Arguments.of(new Class<?>[]{BigDecimal.class}, "BOOLEAN",new Object[]{BigDecimal.valueOf(1)},"BigDecimal")
                ,Arguments.of(new Class<?>[]{BigDecimal.class}, "BIGINT",new Object[]{BigDecimal.valueOf(1)},"BigDecimal")
                ,Arguments.of(new Class<?>[]{BigDecimal.class}, "NUMBER",new Object[]{BigDecimal.valueOf(1)},"BigDecimal")
                ,Arguments.of(new Class<?>[]{BigDecimal.class}, "REAL",new Object[]{BigDecimal.valueOf(1)},"BigDecimal")
                ,Arguments.of(new Class<?>[]{BigDecimal.class}, "VARCHAR(22)",new Object[]{BigDecimal.valueOf(1)},"BigDecimal")

                ,Arguments.of(new Class<?>[]{double.class}, "BOOLEAN",new Object[]{1.0},"Double")
                ,Arguments.of(new Class<?>[]{double.class}, "BIGINT",new Object[]{1.0},"Double")
                ,Arguments.of(new Class<?>[]{double.class}, "NUMBER",new Object[]{1.0},"Double")
                ,Arguments.of(new Class<?>[]{double.class}, "REAL",new Object[]{1.0},"Double")
                ,Arguments.of(new Class<?>[]{double.class}, "VARCHAR(22)",new Object[]{1.0},"Double")
                ,Arguments.of(new Class<?>[]{double.class}, "INTEGER",new Object[]{1.0},"Double")

                ,Arguments.of(new Class<?>[]{float.class}, "BOOLEAN",new Object[]{1.0F},"Float")
                ,Arguments.of(new Class<?>[]{float.class}, "BIGINT",new Object[]{1.0F},"Float")
                ,Arguments.of(new Class<?>[]{float.class}, "NUMBER",new Object[]{1.0F},"Float")
                ,Arguments.of(new Class<?>[]{float.class}, "REAL",new Object[]{1.0F},"Float")
                ,Arguments.of(new Class<?>[]{float.class}, "VARCHAR(22)",new Object[]{1.0F},"Float")
                ,Arguments.of(new Class<?>[]{float.class}, "INTEGER",new Object[]{1.0F},"Float")
                ,Arguments.of(new Class<?>[]{float.class}, "FLOAT",new Object[]{1.0F},"Float")

                ,Arguments.of(new Class<?>[]{long.class}, "BOOLEAN",new Object[]{1L},"Long")
                ,Arguments.of(new Class<?>[]{long.class}, "BIGINT",new Object[]{1L},"Long")
                ,Arguments.of(new Class<?>[]{long.class}, "NUMBER",new Object[]{1L},"Long")
                ,Arguments.of(new Class<?>[]{long.class}, "REAL",new Object[]{1L},"Long")
                ,Arguments.of(new Class<?>[]{long.class}, "VARCHAR(22)",new Object[]{1L},"Long")
                ,Arguments.of(new Class<?>[]{long.class}, "INTEGER",new Object[]{1L},"Long")
                ,Arguments.of(new Class<?>[]{long.class}, "FLOAT",new Object[]{1L},"Long")

                ,Arguments.of(new Class<?>[]{int.class}, "BOOLEAN",new Object[]{1},"Int")
                ,Arguments.of(new Class<?>[]{int.class}, "BIGINT",new Object[]{1},"Int")
                ,Arguments.of(new Class<?>[]{int.class}, "NUMBER",new Object[]{1},"Int")
                ,Arguments.of(new Class<?>[]{int.class}, "REAL",new Object[]{1},"Int")
                ,Arguments.of(new Class<?>[]{int.class}, "VARCHAR(22)",new Object[]{1},"Int")
                ,Arguments.of(new Class<?>[]{int.class}, "INTEGER",new Object[]{1},"Int")
                ,Arguments.of(new Class<?>[]{int.class}, "FLOAT",new Object[]{1},"Int")

                ,Arguments.of(new Class<?>[]{short.class}, "BOOLEAN",new Object[]{(short)1},"Short")
                ,Arguments.of(new Class<?>[]{short.class}, "BIGINT",new Object[]{(short)1},"Short")
                ,Arguments.of(new Class<?>[]{short.class}, "NUMBER",new Object[]{(short)1},"Short")
                ,Arguments.of(new Class<?>[]{short.class}, "REAL",new Object[]{(short)1},"Short")
                ,Arguments.of(new Class<?>[]{short.class}, "VARCHAR(22)",new Object[]{(short)1},"Short")
                ,Arguments.of(new Class<?>[]{short.class}, "INTEGER",new Object[]{(short)1},"Short")
                ,Arguments.of(new Class<?>[]{short.class}, "FLOAT",new Object[]{(short)1},"Short")

                ,Arguments.of(new Class<?>[]{byte.class}, "BOOLEAN",new Object[]{(byte)1},"Byte")
                ,Arguments.of(new Class<?>[]{byte.class}, "BIGINT",new Object[]{(byte)1},"Byte")
                ,Arguments.of(new Class<?>[]{byte.class}, "NUMBER",new Object[]{(byte)1},"Byte")
                ,Arguments.of(new Class<?>[]{byte.class}, "REAL",new Object[]{(byte)1},"Byte")
                ,Arguments.of(new Class<?>[]{byte.class}, "VARCHAR(22)",new Object[]{(byte)1},"Byte")
                ,Arguments.of(new Class<?>[]{byte.class}, "INTEGER",new Object[]{(byte)1},"Byte")
                ,Arguments.of(new Class<?>[]{byte.class}, "FLOAT",new Object[]{(byte)1},"Byte")

        );
    }

    @ParameterizedTest
    @MethodSource("provideTestStatements")
    void testStatements(Class<?>[] input, String sqlType,Object[] value,String method) throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        var tableName = "testStatements"+method;

        var mainConn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        mainConn.createStatement().execute("create table if not exists " +
                tableName +
                " (id IDENTITY NOT NULL PRIMARY KEY, " +
                "val "+sqlType+");");

        var conn = driver.connect(CONNECT_URL,null);
        var ps = conn.prepareStatement("INSERT INTO "+tableName+" (val) VALUES ( ?)");

        invokeSet(ps,input,value,method,1);
        ps.executeUpdate();

        var ss = conn.prepareStatement("SELECT * FROM "+tableName+" WHERE val=?");
        invokeSet(ss,input,value,method,1);
        var rs = ss.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
        System.out.println("VALUE "+value[0].toString());
            assertEquals(value[0].toString(), invokeGet(rs,  method, 2).toString());
            assertEquals(value[0].toString(), invokeGet(rs,  method, "val").toString());
    }



    private static Stream<Arguments> provideTestBooleanAutoCast() throws MalformedURLException {
        return Stream.of(
                Arguments.of(new Class<?>[]{int.class}, "INTEGER",new Object[]{1},"Int",true),
                Arguments.of(new Class<?>[]{String.class}, "VARCHAR(255)",new Object[]{"TrUe"},"String",true),
                Arguments.of(new Class<?>[]{byte.class}, "TINYINT",new Object[]{(byte)1},"Byte",true),
                Arguments.of(new Class<?>[]{double.class}, "DOUBLE PRECISION",new Object[]{1.0},"Double",true),
                Arguments.of(new Class<?>[]{float.class}, "REAL",new Object[]{1.0F},"Float",true),
                Arguments.of(new Class<?>[]{short.class}, "SMALLINT",new Object[]{(short)22},"Short",true),
                Arguments.of(new Class<?>[]{boolean.class}, "BOOLEAN",new Object[]{true},"Boolean",true),
                Arguments.of(new Class<?>[]{boolean.class}, "BOOLEAN",new Object[]{false},"Boolean",false),
                Arguments.of(new Class<?>[]{int.class}, "INTEGER",new Object[]{0},"Int",false),
                Arguments.of(new Class<?>[]{String.class}, "VARCHAR(255)",new Object[]{"0"},"String",false),
                Arguments.of(new Class<?>[]{byte.class}, "TINYINT",new Object[]{(byte)0},"Byte",false),
                Arguments.of(new Class<?>[]{double.class}, "DOUBLE PRECISION",new Object[]{0.0},"Double",false),
                Arguments.of(new Class<?>[]{float.class}, "REAL",new Object[]{0.0F},"Float",false),
                Arguments.of(new Class<?>[]{short.class}, "SMALLINT",new Object[]{(short)0},"Short",false),


                Arguments.of(new Class<?>[]{long.class}, "BIGINT",new Object[]{22L},"Long",true),
                Arguments.of(new Class<?>[]{long.class}, "BIGINT",new Object[]{0L},"Long",false),
                Arguments.of(new Class<?>[]{BigDecimal.class}, "NUMERIC",new Object[]{BigDecimal.valueOf(22)},"BigDecimal",true),
                Arguments.of(new Class<?>[]{BigDecimal.class}, "NUMERIC",new Object[]{BigDecimal.valueOf(0)},"BigDecimal",false)
        );
    }
    @ParameterizedTest
    @MethodSource("provideTestBooleanAutoCast")
    void testBooleanAutoCast(Class<?>[] input, String sqlType,Object[] value,String method,Object expected) throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        var tableName = "testStatements"+method;

        var mainConn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        mainConn.createStatement().execute("create table if not exists " +
                tableName +
                " (id IDENTITY NOT NULL PRIMARY KEY, " +
                "val "+sqlType+");");

        var conn = driver.connect(CONNECT_URL,null);
        var ps = conn.prepareStatement("INSERT INTO "+tableName+" (val) VALUES ( ?)");

        invokeSet(ps,input,value,method,1);
        ps.executeUpdate();

        var ss = conn.prepareStatement("SELECT * FROM "+tableName+" WHERE val=?");
        invokeSet(ss,input,value,method,1);
        var rs = ss.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
        System.out.println("VALUE "+value[0].toString());
        var target = new Class[]{boolean.class};
        var targetMethod = "Boolean";
        assertEquals(expected.toString(), invokeGet(rs,  targetMethod, 2).toString());
        assertEquals(expected.toString(), invokeGet(rs,  targetMethod, "val").toString());
    }

    private  void insertClobprepareStatement(Connection connection, int mb) throws IOException, SQLException {

        var resultBytes = getCharacters( mb);


        PreparedStatement st = connection.prepareStatement(
                "insert into CLOBTAB (ID, DATA) values (?, ?)");
        var clob = connection.createClob();
        clob.setString(1,new String(resultBytes));
        st.setInt(1, mb);
        st.setClob(2, clob);
        st.executeUpdate();
    }

    @Test
    public void testinsertClobprepareStatement() throws SQLException, IOException {
        var conn = driver.connect(CONNECT_URL,null);
        try (PreparedStatement st = conn.prepareStatement(
                "create table if not exists CLOBTAB (ID integer, DATA CLOB)")) {
            st.executeUpdate();
        }
        insertClobprepareStatement(conn, 1);
        insertClobprepareStatement(conn, 2);
        var stmt = conn.createStatement();
        ResultSet rs = null;
        if (stmt.execute("SELECT DATA FROM CLOBTAB")) {
            rs = stmt.getResultSet();
        }
        assertNotNull(rs);
        rs.next();
        var data = rs.getClob(1);
        assertEquals(1000, IOUtils.toByteArray(data.getAsciiStream()).length);
        data = rs.getClob("data");
        assertEquals(1000, IOUtils.toByteArray(data.getAsciiStream()).length);

        rs.next();
        data = rs.getClob(1);
        assertEquals(2000, IOUtils.toByteArray(data.getAsciiStream()).length);
        data = rs.getClob("data");
        assertEquals(2000, IOUtils.toByteArray(data.getAsciiStream()).length);
    }




    private  void insertBlobprepareStatement(Connection connection, int mb) throws IOException, SQLException {

        var resultBytes = getBytes( mb);


        PreparedStatement st = connection.prepareStatement(
                "insert into BLOBTAB (ID, DATA) values (?, ?)");
        var clob = connection.createBlob();
        clob.setBytes(1,resultBytes);
        st.setInt(1, mb);
        st.setBlob(2, clob);
        st.executeUpdate();
    }
    @Test
    public void testinsertBlobprepareStatement() throws SQLException, IOException {
        var conn = driver.connect(CONNECT_URL,null);
        try (PreparedStatement st = conn.prepareStatement(
                "create table if not exists BLOBTAB (ID integer, DATA BLOB)")) {
            st.executeUpdate();
        }
        insertBlobprepareStatement(conn, 1);
        insertBlobprepareStatement(conn, 2);
        var stmt = conn.createStatement();
        ResultSet rs = null;
        if (stmt.execute("SELECT DATA FROM BLOBTAB")) {
            rs = stmt.getResultSet();
        }
        assertNotNull(rs);
        rs.next();
        var data = rs.getBlob(1);
        assertEquals(1000, IOUtils.toByteArray(data.getBinaryStream()).length);
        data = rs.getBlob("data");
        assertEquals(1000, IOUtils.toByteArray(data.getBinaryStream()).length);

        rs.next();
        data = rs.getBlob(1);
        assertEquals(2000, IOUtils.toByteArray(data.getBinaryStream()).length);
        data = rs.getBlob("data");
        assertEquals(2000, IOUtils.toByteArray(data.getBinaryStream()).length);
    }



    private  void insertNClobprepareStatement(Connection connection, int mb) throws IOException, SQLException {

        var resultBytes = getCharacters( mb);


        PreparedStatement st = connection.prepareStatement(
                "insert into NCLOBTAB (ID, DATA) values (?, ?)");
        var clob = connection.createNClob();
        clob.setString(1,new String(resultBytes));
        st.setInt(1, mb);
        st.setNClob(2, clob);
        st.executeUpdate();
    }

    @Test
    public void testinsertMClobprepareStatement() throws SQLException, IOException {
        var conn = driver.connect(CONNECT_URL,null);
        try (PreparedStatement st = conn.prepareStatement(
                "create table if not exists NCLOBTAB (ID integer, DATA NCLOB)")) {
            st.executeUpdate();
        }
        insertNClobprepareStatement(conn, 1);
        insertNClobprepareStatement(conn, 2);
        var stmt = conn.createStatement();
        ResultSet rs = null;
        if (stmt.execute("SELECT DATA FROM NCLOBTAB")) {
            rs = stmt.getResultSet();
        }
        assertNotNull(rs);
        rs.next();
        var data = rs.getNClob(1);
        assertEquals(1000, IOUtils.toByteArray(data.getAsciiStream()).length);
        data = rs.getNClob("data");
        assertEquals(1000, IOUtils.toByteArray(data.getAsciiStream()).length);

        rs.next();
        data = rs.getNClob(1);
        assertEquals(2000, IOUtils.toByteArray(data.getAsciiStream()).length);
        data = rs.getNClob("data");
        assertEquals(2000, IOUtils.toByteArray(data.getAsciiStream()).length);
    }

    @Test
    public void testDates() throws SQLException {
        var conn = driver.connect(CONNECT_URL,null);
        try (PreparedStatement st = conn.prepareStatement(
                "create table if not exists TIMETAB (ID integer," +
                        " T TIME," +
                        " D DATE," +
                        " TST TIMESTAMP)")) {
            st.executeUpdate();
        }

        PreparedStatement st = conn.prepareStatement(
                "insert into TIMETAB (ID, T,D,TST) values (?, ?,?,?)");
        var calendar = java.util.Calendar.getInstance();
        st.setInt(1, 1);
        var expectedTime = Time.valueOf(LocalTime.ofInstant(calendar.toInstant(), ZoneId.systemDefault()));
        st.setTime(2,expectedTime);
        var expectedDate = Date.valueOf(LocalDate.ofInstant(calendar.toInstant(), ZoneId.systemDefault()));
        st.setDate(3, expectedDate);
        var expectedTimestamp = Timestamp.from(calendar.toInstant());
        st.setTimestamp(4, expectedTimestamp);
        st.executeUpdate();

        var rs = conn.createStatement().executeQuery("SELECT * FROM TIMETAB");
        assertTrue(rs.next());
        assertEquals(expectedTime, rs.getTime(2));
        assertEquals(expectedTime, rs.getTime("t"));
        assertEquals(Date.valueOf("1970-01-01").toString(), rs.getDate(2).toString());
        assertEquals(expectedTime, Time.valueOf(LocalTime.ofInstant(rs.getTimestamp(2).toInstant(), ZoneId.systemDefault())));

        assertEquals(expectedDate, rs.getDate(3));
        assertEquals(expectedDate, rs.getDate("d"));
        assertEquals(Time.valueOf("00:00:00").toString(), rs.getTime(3).toString());
        assertEquals(expectedDate, Date.valueOf(LocalDate.ofInstant(rs.getTimestamp(3).toInstant(), ZoneId.systemDefault())));

        assertEquals(expectedTimestamp, rs.getTimestamp(4));
        assertEquals(expectedTimestamp, rs.getTimestamp("tst"));
        assertEquals(expectedTime, rs.getTime(4));
        assertEquals(expectedDate.toString(), rs.getDate(4).toString());


        assertEquals(expectedTimestamp, rs.getObject(4,Timestamp.class));

    }
}
