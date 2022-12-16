package org.h2.test;

import org.junit.jupiter.api.Assertions;
import org.kendar.janus.utils.TestBase;

import java.lang.reflect.Method;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDb extends TestBase {

    public void execute(PreparedStatement stat) throws SQLException {
        execute(stat, null);
    }
    protected Connection getConnection(String name) throws SQLException {
        return driver.connect(CONNECT_URL,null);
    }

    protected void assertKnownException(String message, SQLException e) {
        if (e != null && e.getSQLState().startsWith("HY000")) {
            System.out.println("Unexpected General error " + message+ e);
        }
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


    /**
     * Check if a result set contains the expected data.
     * The sort order is significant
     *
     * @param rs the result set
     * @param data the expected data
     * @param ignoreColumns columns to ignore, or {@code null}
     * @throws AssertionError if there is a mismatch
     */
    protected void assertResultSetOrdered(ResultSet rs, String[][] data, int[] ignoreColumns)
            throws SQLException {
        assertResultSet(true, rs, data, ignoreColumns);
    }

    /**
     * Check if a result set contains the expected data.
     * The sort order is significant
     *
     * @param rs the result set
     * @param data the expected data
     * @throws AssertionError if there is a mismatch
     */
    protected void assertResultSetOrdered(ResultSet rs, String[][] data)
            throws SQLException {
        assertResultSet(true, rs, data, null);
    }

    /**
     * Check if a result set contains the expected data.
     *
     * @param ordered if the sort order is significant
     * @param rs the result set
     * @param data the expected data
     * @param ignoreColumns columns to ignore, or {@code null}
     * @throws AssertionError if there is a mismatch
     */
    private void assertResultSet(boolean ordered, ResultSet rs, String[][] data, int[] ignoreColumns)
            throws SQLException {
        int len = rs.getMetaData().getColumnCount();
        int rows = data.length;
        if (rows == 0) {
            // special case: no rows
            if (rs.next()) {
                fail("testResultSet expected rowCount:" + rows + " got:0");
            }
        }
        int len2 = data[0].length;
        if (len < len2) {
            fail("testResultSet expected columnCount:" + len2 + " got:" + len);
        }
        for (int i = 0; i < rows; i++) {
            if (!rs.next()) {
                fail("testResultSet expected rowCount:" + rows + " got:" + i);
            }
            String[] row = getData(rs, len);
            if (ordered) {
                String[] good = data[i];
                if (!testRow(good, row, good.length, ignoreColumns)) {
                    fail("testResultSet row not equal, got:\n" + formatRow(row)
                            + "\n" + formatRow(good));
                }
            } else {
                boolean found = false;
                for (int j = 0; j < rows; j++) {
                    String[] good = data[i];
                    if (testRow(good, row, good.length, ignoreColumns)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    fail("testResultSet no match for row:" + formatRow(row));
                }
            }
        }
        if (rs.next()) {
            String[] row = getData(rs, len);
            fail("testResultSet expected rowcount:" + rows + " got:>="
                    + (rows + 1) + " data:" + formatRow(row));
        }
    }

    private static boolean testRow(String[] a, String[] b, int len, int[] ignoreColumns) {
        loop: for (int i = 0; i < len; i++) {
            if (ignoreColumns != null) {
                for (int c : ignoreColumns) {
                    if (c == i) {
                        continue loop;
                    }
                }
            }
            String sa = a[i];
            String sb = b[i];
            if (sa == null || sb == null) {
                if (sa != sb) {
                    return false;
                }
            } else {
                if (!sa.equalsIgnoreCase(sb)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static String formatRow(String[] row) {
        String sb = "";
        for (String r : row) {
            sb += "{" + r + "}";
        }
        return "{" + sb + "}";
    }

    private static String[] getData(ResultSet rs, int len) throws SQLException {
        String[] data = new String[len];
        for (int i = 0; i < len; i++) {
            data[i] = rs.getString(i + 1);
            // just check if it works
            rs.getObject(i + 1);
        }
        return data;
    }

    protected void assertResultSetMeta(ResultSet rs, int columnCount,
                                       String[] labels, int[] datatypes, int[] precision, int[] scale)
            throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cc = meta.getColumnCount();
        if (cc != columnCount) {
            fail("result set contains " + cc + " columns not " + columnCount);
        }
        for (int i = 0; i < columnCount; i++) {
            if (labels != null) {
                String l = meta.getColumnLabel(i + 1);
                if (!labels[i].equals(l)) {
                    fail("column label " + i + " is " + l + " not " + labels[i]);
                }
            }
            if (datatypes != null) {
                int t = meta.getColumnType(i + 1);
                if (datatypes[i] != t) {
                    fail("column datatype " + i + " is " + t + " not " + datatypes[i] + " (prec="
                            + meta.getPrecision(i + 1) + " scale=" + meta.getScale(i + 1) + ")");
                }
                String typeName = meta.getColumnTypeName(i + 1);
                String className = meta.getColumnClassName(i + 1);
                switch (t) {
                    case Types.INTEGER:
                        assertEquals("INTEGER", typeName);
                        assertEquals("java.lang.Integer", className);
                        break;
                    case Types.VARCHAR:
                        assertEquals("CHARACTER VARYING", typeName);
                        assertEquals("java.lang.String", className);
                        break;
                    case Types.SMALLINT:
                        assertEquals("SMALLINT", typeName);
                        assertEquals("java.lang.Integer", className);
                        break;
                    case Types.TIMESTAMP:
                        assertEquals("TIMESTAMP", typeName);
                        assertEquals("java.sql.Timestamp", className);
                        break;
                    case Types.NUMERIC:
                        assertEquals("NUMERIC", typeName);
                        assertEquals("java.math.BigDecimal", className);
                        break;
                    default:
                }
            }
            if (precision != null) {
                int p = meta.getPrecision(i + 1);
                if (precision[i] != p) {
                    fail("column precision " + i + " is " + p + " not " + precision[i]);
                }
            }
            if (scale != null) {
                int s = meta.getScale(i + 1);
                if (scale[i] != s) {
                    fail("column scale " + i + " is " + s + " not " + scale[i]);
                }
            }

        }
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
