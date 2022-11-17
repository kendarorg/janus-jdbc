package org.kendar.janus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.janus.utils.TestBase;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

public class PStatementTest extends TestBase {

    @BeforeEach
    protected void beforeEach() throws SQLException {
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }

    @Test
    public void testGetGeneratedKeys() throws SQLException {
        createSimpleTable();
        var conn = driver.connect(CONNECT_URL,null);
        PreparedStatement statement = conn.prepareStatement(
                "insert into persons (name) values (?)",
                Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, "Foo");
        int affectedRows = statement.executeUpdate();
        assertTrue(affectedRows>0);

        ResultSet keys = statement.getGeneratedKeys();
        assertTrue(keys.next());
        var key = keys.getLong(1);
        assertTrue(key>=1);
    }

    @Test
    public void testStatementExecuteQuery() throws  SQLException {
        createSimpleTable();
        insertInSimpleTable("A");
        var conn = driver.connect(CONNECT_URL,null);
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT * FROM persons");

        assertTrue(rs.next());
        assertEquals("A",rs.getString(2));
        assertEquals("A",rs.getString("name"));

        assertFalse(rs.next());

    }


    @Test
    public void testArray() throws SQLException {
        var dbConn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        dbConn.createStatement().execute("create table if not exists " +
                "arrays (\n" +
                "    id IDENTITY NOT NULL PRIMARY KEY,\n" +
                "    col INTEGER ARRAY\n" +
                "  );");
        var conn = driver.connect(CONNECT_URL,null);
        var ps = conn.prepareStatement("INSERT INTO arrays (col) VALUES ( ?)");
        var srcArray = conn.createArrayOf("INTEGER",new Object[]{1,2,3});
        ps.setArray(1,srcArray);
        ps.executeUpdate();

        var rs =conn.createStatement().executeQuery("SELECT * FROM arrays");
        assertTrue(rs.next());
        var res = rs.getArray(2);
        assertNotNull(res);
        var arrayResult = (Object[])res.getArray();
        assertEquals(3,arrayResult.length);
        assertEquals(1,(int)arrayResult[0]);
        assertEquals(2,(int)arrayResult[1]);
        assertEquals(3,(int)arrayResult[2]);


        res = rs.getArray("col");
        assertNotNull(res);
        arrayResult = (Object[])res.getArray();
        assertEquals(3,arrayResult.length);
        assertEquals(1,(int)arrayResult[0]);
        assertEquals(2,(int)arrayResult[1]);
        assertEquals(3,(int)arrayResult[2]);
    }

    @Test
    public void testExecuteBatchStatementBatch() throws  SQLException {
        createSimpleTable();
        var conn = driver.connect(CONNECT_URL,null);

        var statement = conn.prepareStatement(
                "INSERT INTO persons(NAME) VALUES (?)"
        );
        statement.setString(1, "testExecuteBatchStatement00");
        statement.addBatch();
        statement.setString(1, "testExecuteBatchStatement01");
        // Add row to the batch.
        statement.addBatch();
        statement.executeBatch();

        ResultSet rs = null;
        var str = conn.createStatement();
        if (str.execute("SELECT count(*) FROM persons WHERE NAME LIKE 'testExecuteBatchStatement0%'")) {
            rs = str.getResultSet();
        }
        Assertions.assertNotNull(rs);
        Assertions.assertTrue(rs.next());
        assertEquals(2,rs.getLong(1));
    }

    @Test
    void nullsSet() throws SQLException, InterruptedException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        //FIXME: https://www.tutorialspoint.com/what-is-concur-updatable-resultset-in-jdbc-explain
        var dbc = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        dbc.createStatement().execute("create table if not exists " +
                "persons(" +
                "id IDENTITY NOT NULL PRIMARY KEY, " +
                "name varchar(255));");
        dbc.createStatement().execute("INSERT INTO persons(NAME) VALUES ('A');");
        dbc.createStatement().execute("INSERT INTO persons(NAME) VALUES ('B');");

        var conn = driver.connect(CONNECT_URL,null);


        var statement = conn.prepareStatement("UPDATE persons SET name=? WHERE name='A';");
        assertNotNull(statement);
        statement.setNull(1,Types.VARCHAR);
        statement.executeUpdate();

        var rs= conn.createStatement().executeQuery("SELECT * FROM persons WHERE name IS NULL ORDER BY NAME ASC");
        assertTrue(rs.next());
        assertEquals(null,rs.getString("name"));
    }



    @Test
    void nullsSetWithWeirdValues() throws SQLException, InterruptedException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        //FIXME: https://www.tutorialspoint.com/what-is-concur-updatable-resultset-in-jdbc-explain
        var dbc = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        dbc.createStatement().execute("create table if not exists " +
                "persons(" +
                "id IDENTITY NOT NULL PRIMARY KEY, " +
                "name varchar(255));");
        dbc.createStatement().execute("INSERT INTO persons(NAME) VALUES ('A');");
        dbc.createStatement().execute("INSERT INTO persons(NAME) VALUES ('B');");

        var conn = driver.connect(CONNECT_URL,null);


        var statement = conn.prepareStatement("UPDATE persons SET name=? WHERE name='A';");
        assertNotNull(statement);
        statement.setNull(1,Types.FLOAT,"RANDOM");
        statement.executeUpdate();

        var rs= conn.createStatement().executeQuery("SELECT * FROM persons WHERE name IS NULL ORDER BY NAME ASC");
        assertTrue(rs.next());
        assertEquals(null,rs.getString("name"));
    }
}
