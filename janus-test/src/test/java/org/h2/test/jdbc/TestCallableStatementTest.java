/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayInputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;

import org.h2.test.TestDb;
import org.h2.tools.SimpleResultSet;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the CallableStatement class.
 */
@SuppressWarnings("ALL")
public class TestCallableStatementTest extends TestDb {


    @BeforeEach
    protected void beforeEach() throws SQLException {
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }


    @Test
    public void testOutParameter() throws SQLException {
        var conn = getConnection("callableStatement");
        conn.createStatement().execute("CREATE SEQUENCE SEQ");
        for (int i = 1; i < 20; i++) {
            CallableStatement cs = conn.prepareCall("{ ? = CALL NEXT VALUE FOR SEQ}");
            cs.registerOutParameter(1, Types.BIGINT);
            cs.execute();
            long id = cs.getLong(1);
            Assertions.assertEquals(i, id);
            cs.close();
        }
        conn.createStatement().execute("DROP SEQUENCE SEQ");
    }

    @Test
    public void testUnsupportedOperations() throws SQLException {

        var conn = getConnection("callableStatement");
        CallableStatement call;
        call = conn.prepareCall("select 10 as a");
        assertThrows(Exception.class,()-> call.
                getURL(1));
        assertThrows(Exception.class, ()->call.
                getObject(1, Collections.emptyMap()));
        assertThrows(Exception.class, ()->call.
                getRef(1));
        assertThrows(Exception.class, ()->call.
                getRowId(1));

        assertThrows(Exception.class, ()->call.
                getURL("a"));
        assertThrows(Exception.class,()-> call.
                getObject("a", Collections.emptyMap()));
        assertThrows(Exception.class, ()->call.
                getRef("a"));
        assertThrows(Exception.class,()-> call.
                getRowId("a"));

//FIXME        assertThrows(Exception.class, ()->call.
//                setURL(1, (URL) null));
//        assertThrows(Exception.class, ()->call.
//                setRef(1, (Ref) null));
//        assertThrows(Exception.class, ()->call.
//                setRowId(1, (RowId) null));

//        assertThrows(Exception.class, ()->call.
//                setURL("a", (URL) null));
//        assertThrows(Exception.class, ()->call.
//                setRowId("a", (RowId) null));
    }

    @Test
    public void testCallWithResultSet() throws SQLException {
        var conn = getConnection("callableStatement");
        CallableStatement call;
        ResultSet rs;
        call = conn.prepareCall("select 10 as a");
        call.execute();
        rs = call.getResultSet();
        rs.next();
        Assertions.assertEquals(10, rs.getInt(1));
    }

    @Test
    public void testPreparedStatement() throws SQLException {
        var conn = getConnection("callableStatement");
        // using a callable statement like a prepared statement
        CallableStatement call;
        call = conn.prepareCall("create table test(id int)");
        call.executeUpdate();
        call = conn.prepareCall("insert into test values(1), (2)");
        Assertions.assertEquals(2, call.executeUpdate());
        call = conn.prepareCall("drop table test");
        call.executeUpdate();
    }


    @Test
    public void testGetters() throws SQLException {

        var conn = getConnection("callableStatement");
        CallableStatement call;
        call = conn.prepareCall("{?=call ?}");
        call.setLong(2, 1);
        call.registerOutParameter(1, Types.BIGINT);
        call.execute();
        Assertions.assertEquals(1, call.getLong(1));
        Assertions.assertEquals(1, call.getByte(1));
        Assertions.assertEquals(1, ((Long) call.getObject(1)).longValue());
        Assertions.assertEquals(1, call.getObject(1, Long.class).longValue());
        assertFalse(call.wasNull());

        call.setFloat(2, 1.1f);
        call.registerOutParameter(1, Types.REAL);
        call.execute();
        Assertions.assertEquals(1.1f, call.getFloat(1));

        call.setDouble(2, Math.PI);
        call.registerOutParameter(1, Types.DOUBLE);
        call.execute();
        Assertions.assertEquals(Math.PI, call.getDouble(1));

        call.setBytes(2, new byte[11]);
        call.registerOutParameter(1, Types.BINARY);
        call.execute();
        Assertions.assertEquals(11, call.getBytes(1).length);
        Assertions.assertEquals(11, call.getBlob(1).length());

        call.setDate(2, java.sql.Date.valueOf("2000-01-01"));
        call.registerOutParameter(1, Types.DATE);
        call.execute();
        Assertions.assertEquals("2000-01-01", call.getDate(1).toString());
        Assertions.assertEquals("2000-01-01", call.getObject(1, LocalDate.class).toString());

        call.setTime(2, java.sql.Time.valueOf("01:02:03"));
        call.registerOutParameter(1, Types.TIME);
        call.execute();
        Assertions.assertEquals("01:02:03", call.getTime(1).toString());
        Assertions.assertEquals("01:02:03", call.getObject(1, LocalTime.class).toString());

        call.setTimestamp(2, java.sql.Timestamp.valueOf(
                "2001-02-03 04:05:06.789"));
        call.registerOutParameter(1, Types.TIMESTAMP);
        call.execute();
        Assertions.assertEquals("2001-02-03 04:05:06.789", call.getTimestamp(1).toString());
        Assertions.assertEquals("2001-02-03T04:05:06.789", call.getObject(1, LocalDateTime.class).toString());

        call.setBoolean(2, true);
        call.registerOutParameter(1, Types.BIT);
        call.execute();
        Assertions.assertEquals(true, call.getBoolean(1));

        call.setShort(2, (short) 123);
        call.registerOutParameter(1, Types.SMALLINT);
        call.execute();
        Assertions.assertEquals(123, call.getShort(1));

        call.setBigDecimal(2, BigDecimal.TEN);
        call.registerOutParameter(1, Types.DECIMAL);
        call.execute();
        Assertions.assertEquals("10", call.getBigDecimal(1).toString());
    }


    @Test
    public void testCallWithResult() throws SQLException {
        var conn = getConnection("callableStatement");
        CallableStatement call;
        for (String s : new String[]{"{?= call abs(?)}",
                " { ? = call abs(?)}", " {? = call abs(?)}"}) {
            call = conn.prepareCall(s);
            call.setInt(2, -3);
            call.registerOutParameter(1, Types.INTEGER);
            call.execute();
            Assertions.assertEquals(3, call.getInt(1));
            call.executeUpdate();
            Assertions.assertEquals(3, call.getInt(1));
        }
    }

    @Test
    public void testPrepare() throws Exception {
        var conn = getConnection("callableStatement");
        Statement stat = conn.createStatement();
        CallableStatement call;
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        call = conn.prepareCall("INSERT INTO TEST VALUES(?, ?)");
        call.setInt(1, 1);
        call.setString(2, "Hello");
        call.execute();
        call = conn.prepareCall("SELECT * FROM TEST",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        rs = call.executeQuery();
        rs.next();
        Assertions.assertEquals(1, rs.getInt(1));
        Assertions.assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        call = conn.prepareCall("SELECT * FROM TEST",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        rs = call.executeQuery();
        rs.next();
        Assertions.assertEquals(1, rs.getInt(1));
        Assertions.assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        stat.execute("CREATE ALIAS testCall FOR '" + getClass().getName() + ".testCall'");


        call = conn.prepareCall("{SELECT * FROM testCall(?, ?, ?, ?)}");
        call.setInt("A", 50);
        call.setString("B", "abc");
        long t = System.currentTimeMillis();
        call.setTimestamp("C", new Timestamp(t));
        call.setTimestamp("D", Timestamp.valueOf("2001-02-03 10:20:30.0"));
        call.registerOutParameter(1, Types.INTEGER);
        call.registerOutParameter("B", Types.VARCHAR);
        call.executeUpdate();
         final var call2 = call;
//FIXME
// assertThrows(Exception.class,()-> call2.getTimestamp("C"));


        call.registerOutParameter(3, Types.TIMESTAMP);
        call.registerOutParameter(4, Types.TIMESTAMP);
        call.executeUpdate();

        Assertions.assertEquals(t + 1, call.getTimestamp(3).getTime());
        Assertions.assertEquals(t + 1, call.getTimestamp("C").getTime());

        Assertions.assertEquals("2001-02-03 10:20:30.0", call.getTimestamp(4).toString());
        Assertions.assertEquals("2001-02-03 10:20:30.0", call.getTimestamp("D").toString());
        Assertions.assertEquals("2001-02-03T10:20:30", call.getObject(4, LocalDateTime.class).toString());
        Assertions.assertEquals("2001-02-03T10:20:30", call.getObject("D", LocalDateTime.class).toString());
        Assertions.assertEquals("10:20:30", call.getTime(4).toString());
        Assertions.assertEquals("10:20:30", call.getTime("D").toString());
        Assertions.assertEquals("10:20:30", call.getObject(4, LocalTime.class).toString());
        Assertions.assertEquals("10:20:30", call.getObject("D", LocalTime.class).toString());
        Assertions.assertEquals("2001-02-03", call.getDate(4).toString());
        Assertions.assertEquals("2001-02-03", call.getDate("D").toString());
        Assertions.assertEquals("2001-02-03", call.getObject(4, LocalDate.class).toString());
        Assertions.assertEquals("2001-02-03", call.getObject("D", LocalDate.class).toString());

        Assertions.assertEquals(100, call.getInt(1));
        Assertions.assertEquals(100, call.getInt("A"));
        Assertions.assertEquals(100, call.getLong(1));
        Assertions.assertEquals(100, call.getLong("A"));
        Assertions.assertEquals("100", call.getBigDecimal(1).toString());
        Assertions.assertEquals("100", call.getBigDecimal("A").toString());
        Assertions.assertEquals(100, call.getFloat(1));
        Assertions.assertEquals(100, call.getFloat("A"));
        Assertions.assertEquals(100, call.getDouble(1));
        Assertions.assertEquals(100, call.getDouble("A"));
        Assertions.assertEquals(100, call.getByte(1));
        Assertions.assertEquals(100, call.getByte("A"));
        Assertions.assertEquals(100, call.getShort(1));
        Assertions.assertEquals(100, call.getShort("A"));
        assertTrue(call.getBoolean(1));
        assertTrue(call.getBoolean("A"));

        Assertions.assertEquals("ABC", call.getString(2));
        Reader r = call.getCharacterStream(2);
        Assertions.assertEquals("ABC", IOUtils.readStringAndClose(r, -1));
        r = call.getNCharacterStream(2);
        Assertions.assertEquals("ABC", IOUtils.readStringAndClose(r, -1));
        Assertions.assertEquals("ABC", call.getString("B"));
        Assertions.assertEquals("ABC", call.getNString(2));
        Assertions.assertEquals("ABC", call.getNString("B"));
        Assertions.assertEquals("ABC", call.getClob(2).getSubString(1, 3));
        Assertions.assertEquals("ABC", call.getClob("B").getSubString(1, 3));
        Assertions.assertEquals("ABC", call.getNClob(2).getSubString(1, 3));
        Assertions.assertEquals("ABC", call.getNClob("B").getSubString(1, 3));
        Assertions.assertEquals("ABC", call.getSQLXML(2).getString());
        Assertions.assertEquals("ABC", call.getSQLXML("B").getString());

        final var call3 = call;
        assertThrows(Exception.class, ()->call3.getString(100));
        assertThrows(Exception.class, ()->call3.getString(0));
        assertThrows(Exception.class, ()->call3.getBoolean("X"));

        call.setCharacterStream("B",
                new StringReader("xyz"));
        call.executeUpdate();
        Assertions.assertEquals("XYZ", call.getString("B"));
        call.setCharacterStream("B",
                new StringReader("xyz-"), 3);
        call.executeUpdate();
        Assertions.assertEquals("XYZ", call.getString("B"));
        call.setCharacterStream("B",
                new StringReader("xyz-"), 3L);
        call.executeUpdate();
        Assertions.assertEquals("XYZ", call.getString("B"));
        call.setAsciiStream("B",
                new ByteArrayInputStream("xyz".getBytes(StandardCharsets.UTF_8)));
        call.executeUpdate();
        Assertions.assertEquals("XYZ", call.getString("B"));
        call.setAsciiStream("B",
                new ByteArrayInputStream("xyz-".getBytes(StandardCharsets.UTF_8)), 3);
        call.executeUpdate();
        Assertions.assertEquals("XYZ", call.getString("B"));
        call.setAsciiStream("B",
                new ByteArrayInputStream("xyz-".getBytes(StandardCharsets.UTF_8)), 3L);
        call.executeUpdate();
        Assertions.assertEquals("XYZ", call.getString("B"));

        call.setClob("B", new StringReader("xyz"));
        call.executeUpdate();
        Assertions.assertEquals("XYZ", call.getString("B"));
        call.setClob("B", new StringReader("xyz-"), 3);
        call.executeUpdate();
        Assertions.assertEquals("XYZ", call.getString("B"));

        call.setNClob("B", new StringReader("xyz"));
        call.executeUpdate();
        Assertions.assertEquals("XYZ", call.getString("B"));
        call.setNClob("B", new StringReader("xyz-"), 3);
        call.executeUpdate();
        Assertions.assertEquals("XYZ-", call.getString("B"));

        call.setString("B", "xyz");
        call.executeUpdate();


        Assertions.assertEquals("XYZ", call.getString("B"));
        call.setNString("B", "xyz");
        call.executeUpdate();
        Assertions.assertEquals("XYZ", call.getString("B"));
        SQLXML xml = conn.createSQLXML();
        xml.setString("<x>xyz</x>");
        call.setSQLXML("B", xml);
        call.executeUpdate();
        Assertions.assertEquals("<X>XYZ</X>", call.getString("B"));

        // test for exceptions after closing
        call.close();

        final var call4 = call;
        assertThrows(Exception.class,()-> call4.
                executeUpdate());
        //FIXME OR NOT assertThrows(Exception.class,()-> call4.
        //FIXME OR NOT         registerOutParameter(1, Types.INTEGER));
        assertThrows(Exception.class, ()->call4.
                getString("X"));
    }

    public void testClassLoader() throws SQLException {
        var conn = getConnection("callableStatement");
        Utils.ClassFactory myFactory = new TestClassFactory();
        JdbcUtils.addClassFactory(myFactory);
        try {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS T_CLASSLOADER FOR 'TestClassFactory.testClassF'");
            ResultSet rs = stat.executeQuery("SELECT T_CLASSLOADER(true)");
            assertTrue(rs.next());
            Assertions.assertEquals(false, rs.getBoolean(1));
        } finally {
            JdbcUtils.removeClassFactory(myFactory);
        }
    }

    public void testArrayArgument() throws SQLException {
        var connection = getConnection("callableStatement");
        Array array = connection.createArrayOf("Int", new Object[] {0, 1, 2});
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE ALIAS getArrayLength FOR '" + getClass().getName() + ".getArrayLength'");

            // test setArray
            try (CallableStatement callableStatement = connection
                    .prepareCall("{call getArrayLength(?)}")) {
                callableStatement.setArray(1, array);
                assertTrue(callableStatement.execute());

                try (ResultSet resultSet = callableStatement.getResultSet()) {
                    assertTrue(resultSet.next());
                    Assertions.assertEquals(3, resultSet.getInt(1));
                    assertFalse(resultSet.next());
                }
            }

            // test setObject
            try (CallableStatement callableStatement = connection
                    .prepareCall("{call getArrayLength(?)}")) {
                callableStatement.setObject(1, array);
                assertTrue(callableStatement.execute());

                try (ResultSet resultSet = callableStatement.getResultSet()) {
                    assertTrue(resultSet.next());
                    Assertions.assertEquals(3, resultSet.getInt(1));
                    assertFalse(resultSet.next());
                }
            }
        } finally {
            array.free();
        }
    }

    public void testArrayReturnValue() throws SQLException {
        var connection = getConnection("callableStatement");
        Integer[][] arraysToTest = new Integer[][] {
                {0, 1, 2},
                {0, 1, 2},
                {0, null, 2},
        };
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE ALIAS arrayIdentiy FOR '" + getClass().getName() + ".arrayIdentiy'");

            for (Integer[] arrayToTest : arraysToTest) {
                Array sqlInputArray = connection.createArrayOf("INTEGER", arrayToTest);
                try {
                    try (CallableStatement callableStatement = connection
                            .prepareCall("{call arrayIdentiy(?)}")) {
                        callableStatement.setArray(1, sqlInputArray);
                        assertTrue(callableStatement.execute());

                        try (ResultSet resultSet = callableStatement.getResultSet()) {
                            assertTrue(resultSet.next());

                            // test getArray()
                            Array sqlReturnArray = resultSet.getArray(1);
                            try {
                                Assertions.assertEquals(
                                        (Object[]) sqlInputArray.getArray(),
                                        (Object[]) sqlReturnArray.getArray());
                            } finally {
                                sqlReturnArray.free();
                            }

                            // test getObject(Array.class)
                            sqlReturnArray = resultSet.getObject(1, Array.class);
                            try {
                                Assertions.assertEquals(
                                        (Object[]) sqlInputArray.getArray(),
                                        (Object[]) sqlReturnArray.getArray());
                            } finally {
                                sqlReturnArray.free();
                            }

                            assertFalse(resultSet.next());
                        }
                    }
                } finally {
                    sqlInputArray.free();
                }

            }
        }
    }

    /**
     * Class factory unit test
     * @param b boolean value
     * @return !b
     */
    public static Boolean testClassF(Boolean b) {
        return !b;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param array the array
     * @return the length of the array
     */
    public static int getArrayLength(Integer[] array) {
        return array == null ? 0 : array.length;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param array the array
     * @return the array
     */
    public static Integer[] arrayIdentiy(Integer[] array) {
        return array;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @param a the value a
     * @param b the value b
     * @param c the value c
     * @param d the value d
     * @return a result set
     */
    public static ResultSet testCall(Connection conn, int a, String b,
                                     Timestamp c, Timestamp d) throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("A", Types.INTEGER, 0, 0);
        rs.addColumn("B", Types.VARCHAR, 0, 0);
        rs.addColumn("C", Types.TIMESTAMP, 0, 0);
        rs.addColumn("D", Types.TIMESTAMP, 0, 0);
        if ("jdbc:columnlist:connection".equals(conn.getMetaData().getURL())) {
            return rs;
        }
        rs.addRow(a * 2, b.toUpperCase(), new Timestamp(c.getTime() + 1), d);
        return rs;
    }

    /**
     * A class factory used for testing.
     */
    static class TestClassFactory implements Utils.ClassFactory {

        @Override
        public boolean match(String name) {
            return name.equals("TestClassFactory");
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            return TestCallableStatementTest.class;
        }
    }



    @Test
    public void testGetters01() throws SQLException {

        var conn = getConnection("callableStatement");
        CallableStatement call;
        call = conn.prepareCall("{?=call ?}");

        call.setTimestamp(2, java.sql.Timestamp.valueOf(
                "2001-02-03 04:05:06.789"));
        call.registerOutParameter(1, Types.TIMESTAMP);
        call.execute();
        Assertions.assertEquals("2001-02-03 04:05:06.789", call.getTimestamp(1).toString());
        Assertions.assertEquals("2001-02-03T04:05:06.789", call.getObject(1, LocalDateTime.class).toString());

        call.setBoolean(2, true);
        call.registerOutParameter(1, Types.BIT);
        call.execute();
        Assertions.assertEquals(true, call.getBoolean(1));

        call.setShort(2, (short) 123);
        call.registerOutParameter(1, Types.SMALLINT);
        call.execute();
        Assertions.assertEquals(123, call.getShort(1));

        call.setBigDecimal(2, BigDecimal.TEN);
        call.registerOutParameter(1, Types.DECIMAL);
        call.execute();
        Assertions.assertEquals("10", call.getBigDecimal(1).toString());
    }

}
