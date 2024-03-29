package org.kendar.janus;

import org.h2.tools.SimpleResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.janus.utils.TestBase;

import java.io.IOException;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CStatementTest extends TestBase {

    @SuppressWarnings("CatchMayIgnoreException")
    @BeforeEach
    protected void beforeEach() throws SQLException {

        super.beforeEach();
        @SuppressWarnings("resource") var conn = driver.connect(CONNECT_URL,null);
        var st = conn.createStatement();
        try {
            st.execute("DROP ALIAS TESTSIMPLESP");
        }catch (Exception ex){}
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }
    public static String getVersion(){
        return "1";
    }

    @Test
    public void testSimpleSp() throws SQLException, IOException {
        @SuppressWarnings("resource") var conn = driver.connect(CONNECT_URL,null);
        var st = conn.createStatement();
        st.execute("CREATE ALIAS testSimpleSp FOR \"org.kendar.janus.CStatementTest.getVersion\"");
        ResultSet rs;
        rs = st.executeQuery("CALL testSimpleSp()");
        assertTrue(rs.next());
        assertEquals("1",rs.getString(1));
        var stx = conn.createStatement();
            stx.execute("DROP ALIAS TESTSIMPLESP");
    }

    public static Clob getClob(Clob clob){
        return clob;
    }

    public static ResultSet getClobRs(Connection conn, Clob clob) throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("result", Types.CLOB, 0, 0);
        rs.addRow(clob);
        return rs;
    }

    @Test
    public void testClobSp() throws SQLException, IOException {
        var data = getInputStreamReader(1);
        @SuppressWarnings("resource") var conn = driver.connect(CONNECT_URL,null);

        var st = conn.createStatement();
        st.execute("CREATE ALIAS testSimpleSp FOR \"org.kendar.janus.CStatementTest.getClob\"");
        ResultSet rs;
        var ps = conn.prepareCall("{ ? = CALL testSimpleSp(?)}");
        ps.registerOutParameter(1, Types.CLOB);
        ps.setClob(2,data);
        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(1000,rs.getClob(1).length());

        var stx = conn.createStatement();
        stx.execute("DROP ALIAS TESTSIMPLESP");
    }



    @Test
    public void testClobSpPArtiallyNamed() throws SQLException, IOException {
        var data = getInputStreamReader(1);
        @SuppressWarnings("resource") var conn = driver.connect(CONNECT_URL,null);

        var st = conn.createStatement();
        st.execute("CREATE ALIAS testSimpleSp FOR \"org.kendar.janus.CStatementTest.getClob\"");
        ResultSet rs;
        var ps = conn.prepareCall("{ ? = CALL testSimpleSp(?)}");
        ps.registerOutParameter("PUBLIC.TESTSIMPLESP(?2)", Types.CLOB);
        ps.setClob(2,data);
        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(1000,rs.getClob(1).length());
        var stx = conn.createStatement();
        stx.execute("DROP ALIAS TESTSIMPLESP");
    }



    @Test
    public void testClobSpRs() throws SQLException, IOException {
        var data = getInputStreamReader(1);
        @SuppressWarnings("resource") var conn = driver.connect(CONNECT_URL,null);

        var st = conn.createStatement();
        st.execute("CREATE ALIAS testSimpleSp FOR \"org.kendar.janus.CStatementTest.getClobRs\"");
        ResultSet rs;
        var ps = conn.prepareCall("{ ? = CALL testSimpleSp(?)}");
        ps.registerOutParameter("result", Types.CLOB);
        ps.setClob(2,data);
        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(1000,rs.getClob("result").length());

        var stx = conn.createStatement();
        stx.execute("DROP ALIAS TESTSIMPLESP");
    }
}
