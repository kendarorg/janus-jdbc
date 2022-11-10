package org.kendar.janus;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.kendar.janus.server.JsonServer;
import org.kendar.janus.server.ServerEngine;
import org.kendar.janus.utils.TestBase;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.sql.*;
import java.util.Locale;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SpecialTests extends TestBase {


    @BeforeEach
    protected void beforeEach(){
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }

    @Test
    public void testGetQueryMeta() throws SQLException {
        var mainConn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        mainConn.createStatement().execute("create table if not exists " +
                "persons(" +
                "id IDENTITY NOT NULL PRIMARY KEY, " +
                "name varchar(255));");
        mainConn.createStatement().execute("INSERT INTO persons(NAME) VALUES ('testGetQueryMeta');");

        insertInSimpleTable("testGetQueryMeta");
        var conn = driver.connect(CONNECT_URL,null);
        var stmt = conn.createStatement();
        ResultSet rs = null;
        if (stmt.execute("SELECT * FROM persons")) {
            rs = stmt.getResultSet();
        }
        var rsMetaData = rs.getMetaData();
        assertEquals(2,rsMetaData.getColumnCount());
        assertEquals("ID",rsMetaData.getColumnLabel(1).toUpperCase(Locale.ROOT));
        assertEquals("ID",rsMetaData.getColumnName(1).toUpperCase(Locale.ROOT));
        assertEquals("PERSONS",rsMetaData.getTableName(1).toUpperCase(Locale.ROOT));
    }


    @Test
    public void testGetStatementMeta() throws SQLException {
        var mainConn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        mainConn.createStatement().execute("create table if not exists " +
                "persons(" +
                "id IDENTITY NOT NULL PRIMARY KEY, " +
                "name varchar(255));");
        mainConn.createStatement().execute("INSERT INTO persons(NAME) VALUES ('testGetQueryMeta');");

        insertInSimpleTable("testGetQueryMeta");
        var conn = driver.connect(CONNECT_URL,null);
        var stmt = conn.prepareStatement("SELECT * FROM persons");
        ResultSetMetaData rsMetaData = null;
        if (stmt.execute()) {
            rsMetaData = stmt.getMetaData();
        }
        assertEquals(2,rsMetaData.getColumnCount());
        assertEquals("ID",rsMetaData.getColumnLabel(1).toUpperCase(Locale.ROOT));
        assertEquals("ID",rsMetaData.getColumnName(1).toUpperCase(Locale.ROOT));
        assertEquals("PERSONS",rsMetaData.getTableName(1).toUpperCase(Locale.ROOT));
    }



    @Test
    public void testGetQueryMetaNoPrefetch() throws SQLException {
        var mainConn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        mainConn.createStatement().execute("create table if not exists " +
                "persons(" +
                "id IDENTITY NOT NULL PRIMARY KEY, " +
                "name varchar(255));");
        mainConn.createStatement().execute("INSERT INTO persons(NAME) VALUES ('testGetQueryMeta');");


        var se = new ServerEngine("jdbc:h2:mem:test;", "sa", "sa");
        se.setPrefetchMetadata(true);
        var js = new JsonServer(se);
        var drv = (Driver)new JdbcDriver(js);

        insertInSimpleTable("testGetQueryMeta");
        var conn = drv.connect(CONNECT_URL,null);
        var stmt = conn.createStatement();
        ResultSet rs = null;
        if (stmt.execute("SELECT * FROM persons")) {
            rs = stmt.getResultSet();
        }
        var rsMetaData = rs.getMetaData();
        assertEquals(2,rsMetaData.getColumnCount());
        assertEquals("ID",rsMetaData.getColumnLabel(1).toUpperCase(Locale.ROOT));
        assertEquals("ID",rsMetaData.getColumnName(1).toUpperCase(Locale.ROOT));
        assertEquals("PERSONS",rsMetaData.getTableName(1).toUpperCase(Locale.ROOT));
    }

    @Test
    public void testCancelStatement() throws Exception {

        String createStatement = "create table if not exists testCancelStatement  (foo varchar(50))";
        var dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        dbConnection.createStatement().executeUpdate(createStatement);
        dbConnection.createStatement().execute("INSERT INTO testCancelStatement (foo) VALUES('a')");
        dbConnection.createStatement().execute("INSERT INTO testCancelStatement (foo) VALUES('b')");

        var conn = driver.connect(CONNECT_URL,null);
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT foo FROM testCancelStatement ORDER BY foo ASC");
        Assertions.assertNotNull(rs);
        var count = 0;
        while(rs.next()){
            count++;
            assertEquals("a",rs.getString(1));
            stmt.cancel();
        }
        assertEquals(1,count);
    }

    private static Stream<Arguments> testLobData() throws MalformedURLException {
        return Stream.of(
                Arguments.of("clob"),
                Arguments.of("blob"),
                Arguments.of("nclob"),
                Arguments.of("varchar(100)"),
                Arguments.of("binary(100)")
        );
    }

    @ParameterizedTest
    @MethodSource("testLobData")
    public void testLob(String type) throws SQLException, IOException {
        var dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        var stat = dbConnection.createStatement();
        stat.execute("set max_length_inplace_lob 5");
        stat.execute("create table lob(data "+type+")");
        stat.execute("insert into lob values(space(100))");
        var conn2 = driver.connect(CONNECT_URL,null);
        var stat2 = conn2.createStatement();
        ResultSet rs = stat2.executeQuery("select data from lob");
        rs.next();
        stat.execute("delete lob");
        InputStream in = rs.getBinaryStream(1);
        var result = IOUtils.toByteArray(in);
        assertEquals(100,result.length);
        assertEquals(32,result[0]);
        conn2.close();
    }

    @ParameterizedTest
    @MethodSource("testLobData")
    public void testLobString(String type) throws SQLException, IOException {
        var dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        var stat = dbConnection.createStatement();
        stat.execute("set max_length_inplace_lob 5");
        stat.execute("create table lob(data "+type+")");
        stat.execute("insert into lob values(space(100))");
        var conn2 = driver.connect(CONNECT_URL,null);
        var stat2 = conn2.createStatement();
        ResultSet rs = stat2.executeQuery("select data from lob");
        rs.next();
        stat.execute("delete lob");
        InputStream in = rs.getBinaryStream("data");
        var result = IOUtils.toByteArray(in);
        assertEquals(100,result.length);
        assertEquals(32,result[0]);
        conn2.close();
    }


    @ParameterizedTest
    @MethodSource("testLobData")
    public void testAsciiString(String type) throws SQLException, IOException {
        var dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        var stat = dbConnection.createStatement();
        stat.execute("set max_length_inplace_lob 5");
        stat.execute("create table lob(data "+type+")");
        stat.execute("insert into lob values(space(100))");
        var conn2 = driver.connect(CONNECT_URL,null);
        var stat2 = conn2.createStatement();
        ResultSet rs = stat2.executeQuery("select data from lob");
        rs.next();
        stat.execute("delete lob");
        InputStream in = rs.getAsciiStream("data");
        var result = IOUtils.toByteArray(in);
        assertEquals(100,result.length);
        assertEquals(32,result[0]);
        conn2.close();
    }



    @ParameterizedTest
    @MethodSource("testLobData")
    public void testUnicodeString(String type) throws SQLException, IOException {
        var dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        var stat = dbConnection.createStatement();
        stat.execute("set max_length_inplace_lob 5");
        stat.execute("create table lob(data "+type+")");
        stat.execute("insert into lob values(space(100))");
        var conn2 = driver.connect(CONNECT_URL,null);
        var stat2 = conn2.createStatement();
        ResultSet rs = stat2.executeQuery("select data from lob");
        rs.next();
        stat.execute("delete lob");
        InputStream in = rs.getUnicodeStream("data");
        var result = IOUtils.toByteArray(in);
        assertEquals(100,result.length);
        assertEquals(32,result[0]);
        conn2.close();
    }

    @Test
    public void testRowId() throws  SQLException {
        createSimpleTable();
        insertInSimpleTable("A");
        insertInSimpleTable("B");
        insertInSimpleTable("C");
        var conn = driver.connect(CONNECT_URL,null);

        var statement = conn.createStatement();
        var rs = statement.executeQuery("SELECT _ROWID_,* FROM persons");
        rs.next();
        rs.next();
        var rowId = rs.getRowId("_rowid_");
        var name = rs.getString("name");
        assertEquals("B",name);
        rs.close();

        var ps = conn.prepareStatement("SELECT _ROWID_,* FROM persons WHERE _ROWID_=?");
        ps.setRowId(1,rowId);
        rs = ps.executeQuery();
        rs.next();
        name = rs.getString("name");
        assertEquals("B",name);
    }



    @Test
    public void testRowIdcompositeKey() throws  SQLException {
        var dbc = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        dbc.createStatement().execute("create table if not exists " +
                "persons(" +
                "id BIGINT NOT NULL, " +
                "oth BIGINT NOT NULL, " +
                "name varchar(255));");
        dbc.createStatement().execute("ALTER TABLE persons ADD PRIMARY KEY (id,oth);");
        dbc.createStatement().execute("INSERT INTO persons(ID,OTH,NAME) VALUES (1,1,'A');");
        dbc.createStatement().execute("INSERT INTO persons(ID,OTH,NAME) VALUES (1,2,'B');");
        dbc.createStatement().execute("INSERT INTO persons(ID,OTH,NAME) VALUES (1,3,'C');");
        var conn = driver.connect(CONNECT_URL,null);

        var statement = conn.createStatement();
        var rs = statement.executeQuery("SELECT _ROWID_,NAME FROM persons ORDER BY NAME ASC");
        rs.next();
        rs.next();
        rs.next();
        var rowId = rs.getRowId("_rowid_");
        var name = rs.getString("name");
        assertEquals("C",name);
        rs.close();

        var ps = conn.prepareStatement("SELECT _ROWID_,* FROM persons  WHERE _ROWID_=? ORDER BY NAME DESC");
        ps.setRowId(1,rowId);
        rs = ps.executeQuery();
        rs.next();
        name = rs.getString("name");
        assertEquals("C",name);
    }
}
