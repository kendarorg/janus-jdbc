package org.kendar.janus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.kendar.janus.utils.TestBase;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("resource")
public class StatementTest extends TestBase {

    @BeforeEach
    protected void beforeEach() throws SQLException {
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }

    @Test
    void testConnectClose() throws SQLException {
        var result = driver.connect(CONNECT_URL,null);
        result.close();
        assertNotNull(result);
    }

    @Test
    void testCreateStatement() throws SQLException {
        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement();
        assertNotNull(statement);
        statement.close();
        conn.close();
    }

    @Test
    void testExecuteStatement() throws SQLException {
        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement();
        assertNotNull(statement);
        var result = statement.execute("create table if not exists persons(id IDENTITY NOT NULL PRIMARY KEY, name varchar(255))");
        assertFalse(result);
        statement.close();
        conn.close();
    }


    @Test
    public void testStatementExecuteQueryGEtObject() throws  SQLException {
        createFooTableWithField("testStatementExecuteQuery");
        var conn = driver.connect(CONNECT_URL,null);
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT foo FROM bar WHERE foo = 'testStatementExecuteQuery'");


        while(rs.next()){
            assertEquals("testStatementExecuteQuery", rs.getObject(1));
        }

    }

    @Test
    void testGetResultSet() throws SQLException {
        createSimpleTable();
        insertInSimpleTable("test");
        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement();
        assertNotNull(statement);
        var result = statement.executeQuery("SELECT * FROM persons");
        assertNotNull(result);
        assertTrue(result.next());
        var name = result.getString(2);
        assertEquals("test",name);
        var id = result.getLong("id");
        assertEquals(1L,id);

        result.close();
        statement.close();
        conn.close();
    }

    private static Stream<Arguments> supportingConcurUpdatableData() throws MalformedURLException {
        return Stream.of(
                Arguments.of(
                        "VARCHAR(255)",
                        new String[]{"'A'","'B'"},
                        new Object[]{"A","A_NEW","C_NEW","B"},
                        new Class<?>[]{String.class},
                        "String",""),
                Arguments.of(
                        "BIGINT",
                        new String[]{"10","20"},
                        new Object[]{10L,11L,31L,20L},
                        new Class<?>[]{long.class},
                        "Long",""),
                Arguments.of(
                        "SMALLINT",
                        new String[]{"10","20"},
                        new Object[]{(short)10,(short)11,(short)31,(short)20},
                        new Class<?>[]{short.class},
                        "Short",""),
                Arguments.of(
                        "INTEGER",
                        new String[]{"10","20"},
                        new Object[]{10,11,31,20},
                        new Class<?>[]{int.class},
                        "Int",""),
                Arguments.of(
                        "REAL",
                        new String[]{"10","20"},
                        new Object[]{10F,11F,31F,20F},
                        new Class<?>[]{float.class},
                        "Float",""),
                Arguments.of(
                        "DOUBLE PRECISION",
                        new String[]{"10","20"},
                        new Object[]{10.0,11.0,31.0,20.0},
                        new Class<?>[]{double.class},
                        "Double",""),
                Arguments.of(
                        "NUMERIC",
                        new String[]{"10","20"},
                        new Object[]{BigDecimal.valueOf(10),BigDecimal.valueOf(11),BigDecimal.valueOf(31),BigDecimal.valueOf(20)},
                        new Class<?>[]{BigDecimal.class},
                        "BigDecimal","")

        );
    }
    @ParameterizedTest
    @MethodSource("supportingConcurUpdatableData")
    void supportingConcurUpdatable(String sqlType,String[] inserts,Object[] data,Class<?>[] input,String method,
        String suffix) throws SQLException, InterruptedException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        //FIXME: https://www.tutorialspoint.com/what-is-concur-updatable-resultset-in-jdbc-explain
        var dbc = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        dbc.createStatement().execute("create table if not exists " +
                "persons(" +
                "id IDENTITY NOT NULL PRIMARY KEY, " +
                "name "+sqlType+");");
        dbc.createStatement().execute("INSERT INTO persons(NAME) VALUES ("+inserts[0]+");");
        dbc.createStatement().execute("INSERT INTO persons(NAME) VALUES ("+inserts[1]+");");

        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        assertNotNull(statement);

        var aString = data[0].toString();
        var a_newValue = data[1];
        var a_newString = data[1].toString();
        var c_newValue = data[2];
        var c_newString = data[2].toString();
        var bString = data[3].toString();

        var rs = statement.executeQuery("SELECT * FROM persons ORDER BY NAME ASC");
        assertTrue(rs.next());
        assertEquals(aString+suffix,invokeGet(rs, method, "name").toString());

        rs.beforeFirst();
        assertTrue(rs.next());
        assertEquals(aString+suffix,invokeGet(rs, method, "name").toString());
        invokeUpdate(rs,input,new Object[]{a_newValue},method,"name");
        rs.updateRow();

        rs.beforeFirst();
        assertTrue(rs.next());
        assertEquals(a_newString,invokeGet(rs, method, "name").toString());


        rs.moveToInsertRow();
        rs.updateLong(1,1000L);
        invokeUpdate(rs,input,new Object[]{c_newValue},method,"name");
        rs.insertRow();

        rs.beforeFirst();
        assertTrue(rs.next());
        assertEquals(a_newString,invokeGet(rs, method, "name").toString());

        assertTrue(rs.next());
        assertEquals(bString+suffix,invokeGet(rs, method, "name").toString());

        var rs3 = statement.executeQuery("SELECT * FROM persons ORDER BY NAME ASC");
        assertTrue(rs3.next());
        assertEquals(a_newString,invokeGet(rs3, method, "name").toString());
        assertTrue(rs3.next());
        assertEquals(bString+suffix,invokeGet(rs3, method, "name").toString());
        assertTrue(rs3.next());
        assertEquals(c_newString,invokeGet(rs3, method, "name").toString());
    }

    @Test
    public void testArray() throws SQLException {
        var dbConn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        dbConn.createStatement().execute("create table if not exists " +
                "arrayses (\n" +
                "    id IDENTITY NOT NULL PRIMARY KEY,\n" +
                "    col INTEGER ARRAY\n" +
                "  );");
        var conn = driver.connect(CONNECT_URL,null);
        conn.createStatement().execute("INSERT INTO arrayses (id, col) VALUES (1, ARRAY[1, 2, 3])");
        var rs =conn.createStatement().executeQuery("SELECT * FROM arrayses");
        assertTrue(rs.next());
        var res = rs.getArray(2);
        assertNotNull(res);
        var arrayResult = (Object[])res.getArray();
        assertEquals(3,arrayResult.length);
        assertEquals(1,(int)arrayResult[0]);
        assertEquals(2,(int)arrayResult[1]);
        assertEquals(3,(int)arrayResult[2]);
    }



    @Test
    void testGetLongResultSet() throws SQLException {
        createSimpleTable();
        insertInSimpleTable("1");
        insertInSimpleTable("2");
        insertInSimpleTable("3");
        insertInSimpleTable("4");
        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement();
        assertNotNull(statement);
        var result = statement.executeQuery("SELECT * FROM persons ORDER BY NAME ASC");
        assertNotNull(result);
        var items =0;
        while(result.next()){
            items++;
            var name = result.getString(2);
            assertEquals(String.valueOf(items),name);
        }
        assertEquals(4,items);
    }

    @Test
    void testResultSetHelpers() throws SQLException {
        createSimpleTable();
        insertInSimpleTable("test");
        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement();
        assertNotNull(statement);
        var result = statement.executeQuery("SELECT * FROM persons");
        assertTrue(result.next());

        var realStatement = result.getStatement();
        assertSame(realStatement,statement);

        assertEquals(2,result.findColumn("name"));
    }




    @Test
    public void testGetGeneratedKeys() throws SQLException {
        createSimpleTable();
        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement();
        int affectedRows = statement.executeUpdate(
                "insert into persons (name) values ('Foo')",
                Statement.RETURN_GENERATED_KEYS);
        assertTrue(affectedRows>0);

        ResultSet keys = statement.getGeneratedKeys();
        assertTrue(keys.next());
        var key = keys.getLong(1);
        assertTrue(key>=1);
    }


    @Test
    public void testExecuteBatchStatement() throws  SQLException {
        createSimpleTable();
        var conn = driver.connect(CONNECT_URL,null);


        Statement statement = conn.createStatement();
        statement.addBatch("INSERT INTO persons(NAME) "
                + "VALUES ('testExecuteBatchStatement00')");
        statement.addBatch("INSERT INTO persons(NAME) "
                + "VALUES ('testExecuteBatchStatement01')");
        statement.executeBatch();

        ResultSet rs = null;
        if (statement.execute("SELECT count(*) FROM persons WHERE NAME LIKE 'testExecuteBatchStatement0%'")) {
            rs = statement.getResultSet();
        }
        Assertions.assertNotNull(rs);
        Assertions.assertTrue(rs.next());
        assertEquals(2,rs.getLong(1));
    }

    //FIXME @Test
    void supportingConcurUpdatableDelete() throws SQLException, InterruptedException {
        //FIXME: https://www.tutorialspoint.com/what-is-concur-updatable-resultset-in-jdbc-explain
        createSimpleTable();
        insertInSimpleTable("A");
        insertInSimpleTable("B");
        insertInSimpleTable("C");

        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        assertNotNull(statement);

        var rs = statement.executeQuery("SELECT * FROM persons ORDER BY NAME ASC");
        assertTrue(rs.next());
        assertEquals("A",rs.getString(2));
        rs.deleteRow();

        rs.beforeFirst();
        assertTrue(rs.next());
        assertEquals("B",rs.getString(2));

    }

    @Test
    void nullsUpdates() throws SQLException, InterruptedException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        //FIXME: https://www.tutorialspoint.com/what-is-concur-updatable-resultset-in-jdbc-explain
        var dbc = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        dbc.createStatement().execute("create table if not exists " +
                "persons(" +
                "id IDENTITY NOT NULL PRIMARY KEY, " +
                "name varchar(255));");
        dbc.createStatement().execute("INSERT INTO persons(NAME) VALUES ('A');");
        dbc.createStatement().execute("INSERT INTO persons(NAME) VALUES ('B');");

        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        assertNotNull(statement);


        var rs = statement.executeQuery("SELECT * FROM persons ORDER BY NAME ASC");
        assertTrue(rs.next());
        assertEquals("A",rs.getString("name"));

        rs.beforeFirst();
        assertTrue(rs.next());
        assertEquals("A",rs.getString("name"));
        rs.updateNull("name");
        rs.updateRow();

        rs.beforeFirst();
        assertTrue(rs.next());
        assertNull(rs.getString("name"));
    }
}
