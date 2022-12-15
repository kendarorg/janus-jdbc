package org.h2.test;

import org.junit.jupiter.api.Assertions;
import org.kendar.janus.utils.TestBase;

import java.lang.reflect.Method;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDb extends TestBase {
    protected Connection getConnection(String name) throws SQLException {
        return driver.connect(CONNECT_URL,null);
    }

    protected void assertContains(String toString, String test_x) {
        assertTrue(toString.contains(test_x));
    }

    protected void assertEquals(Object toString, Object test_x) {
        Assertions.assertEquals(toString,test_x);
    }

    protected void assertEquals(String messsage,Object toString, Object test_x) {
        Assertions.assertEquals(toString,test_x,()->messsage);
    }

    protected void execute(Statement stat, String sql) throws SQLException {
        boolean query = sql == null ? ((PreparedStatement) stat).execute() :
                stat.execute(sql);

        if (query /*&& config.lazy*/) {
            try (ResultSet rs = stat.getResultSet()) {
                while (rs.next()) {
                    // just loop
                }
            }
        }
    }

    protected int getSize(int small, int big) {
        return true ? Integer.MAX_VALUE : true ? big : small;
    }


    protected void assertThrowsStatement(int expectedErrorCode, Statement stat,
                                String sql) {
        try {
            execute(stat, sql);
            fail("Expected error: " + expectedErrorCode);
        } catch (SQLException ex) {
            assertEquals(expectedErrorCode, ex.getErrorCode());
        }
    }

    public String getTestName() {
        return getClass().getSimpleName();
    }
    public void deleteDb(String val){}
    protected void assertNotNull(Object e) {
        Assertions.assertNotNull(e);
    }

    protected void assertNull(Object e) {
        Assertions.assertNull(e);
    }
    protected void assertFalse(Boolean e) {
        Assertions.assertFalse(e);
    }

    protected void assertFalse(String val,Boolean e) {
        Assertions.assertFalse(e,val);
    }
    protected void assertTrue(Boolean e) {
        Assertions.assertTrue(e);
    }
    protected void assertTrue(String val,Boolean e) {
        Assertions.assertTrue(e,val);
    }

    private static String formatMethodCall(Method m, Object... args) {
        StringBuilder builder = new StringBuilder();
        builder.append(m.getName()).append('(');
        for (int i = 0; i < args.length; i++) {
            Object a = args[i];
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(a == null ? "null" : a.toString());
        }
        builder.append(")");
        return builder.toString();
    }

    protected void trace(String val){
        System.out.println(val);
    }

    protected void fail(String val){
        Assertions.assertTrue(false,()->val);
    }
}
