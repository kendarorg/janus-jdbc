package org.kendar.janus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.janus.utils.TestBase;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TransactionsTest extends TestBase {

    @BeforeEach
    protected void beforeEach() throws SQLException {
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }




    @Test
    public void testTransactionExecuteOk() throws SQLException {
        createFooTable();
        var conn = driver.connect(CONNECT_URL,null);
        var stmt = conn.createStatement();
        stmt.execute("BEGIN");

        conn.createStatement().execute("INSERT INTO bar (foo) VALUES('testTransactionExecuteOk')");

        stmt.execute("COMMIT");
        ResultSet rs = null;
        if (stmt.execute("SELECT foo FROM bar WHERE foo = 'testTransactionExecuteOk'")) {
            rs = stmt.getResultSet();
        }
        Assertions.assertNotNull(rs);
        while(rs.next()){
            assertEquals("testTransactionExecuteOk",rs.getString(1));
        }
    }


    @Test
    public void testTransactionInternalOk() throws  SQLException {
        createFooTable();
        var conn = driver.connect(CONNECT_URL,null);
        conn.setAutoCommit(false);
        var stmt = conn.createStatement();

        conn.createStatement().execute("INSERT INTO bar (foo) VALUES('testTransactionExecuteOk')");

        conn.commit();
        ResultSet rs = null;
        if (stmt.execute("SELECT foo FROM bar WHERE foo = 'testTransactionExecuteOk'")) {
            rs = stmt.getResultSet();
        }
        Assertions.assertNotNull(rs);
        while(rs.next()){
            assertEquals("testTransactionExecuteOk",rs.getString(1));
        }
    }

    @Test
    public void testTransactionExecuteFail() throws  SQLException {
        createFooTable();
        var conn = driver.connect(CONNECT_URL,null);
        var stmt = conn.createStatement();
        stmt.execute("BEGIN");

        conn.createStatement().execute("INSERT INTO bar (foo) VALUES('testTransactionExecuteFail')");

        stmt.execute("ROLLBACK ");
        ResultSet rs = null;
        if (stmt.execute("SELECT foo FROM bar WHERE foo = 'testTransactionExecuteFail'")) {
            rs = stmt.getResultSet();
        }
        Assertions.assertNotNull(rs);
        Assertions.assertFalse(rs.next());
    }

    @Test
    public void testTransactionInternalFail() throws  SQLException {
        createFooTable();
        var conn = driver.connect(CONNECT_URL,null);
        conn.setAutoCommit(false);
        var stmt = conn.createStatement();

        conn.createStatement().execute("INSERT INTO bar (foo) VALUES('testTransactionExecuteFail')");

        conn.rollback();
        ResultSet rs = null;
        if (stmt.execute("SELECT foo FROM bar WHERE foo = 'testTransactionExecuteFail'")) {
            rs = stmt.getResultSet();
        }
        Assertions.assertNotNull(rs);
        Assertions.assertFalse(rs.next());
    }
}
