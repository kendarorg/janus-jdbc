package org.kendar.janus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.janus.utils.TestBase;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("resource")
public class SavepointTest extends TestBase {

    @BeforeEach
    protected void beforeEach() throws SQLException {
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }



    @Test
    public void savepointRollback() throws SQLException {

        var dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        String createStatement = "create table if not exists emp(id bigint NOT NULL PRIMARY KEY, name varchar(255), job varchar(255))";

        Statement stmt = dbConnection.createStatement();
        stmt.executeUpdate(createStatement);
        var conn = driver.connect(CONNECT_URL,null);

        stmt = conn.createStatement();
        String query1 = "insert into emp values(5,'name','job')";
        String query2 = "select * from emp";

        conn.setAutoCommit(false);
        Savepoint spt1 = conn.setSavepoint("svpt1");
        stmt.execute(query1);
        ResultSet rs = stmt.executeQuery(query2);
        int no_of_rows = 0;

        while (rs.next()) {
            no_of_rows++;
        }
        assertEquals(1, no_of_rows);
        conn.rollback(spt1);
        conn.commit();
        no_of_rows = 0;
        rs = stmt.executeQuery(query2);

        while (rs.next()) {
            no_of_rows++;
        }
        assertEquals(0, no_of_rows);
    }



    @Test
    public void savepointRelease() throws SQLException {

        var dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        String createStatement = "create table if not exists emp(id bigint NOT NULL PRIMARY KEY, name varchar(255), job varchar(255))";

        Statement stmt = dbConnection.createStatement();
        stmt.executeUpdate(createStatement);
        var conn = driver.connect(CONNECT_URL,null);

        stmt = conn.createStatement();
        String query1 = "insert into emp values(5,'name','job')";
        String query2 = "select * from emp";

        conn.setAutoCommit(false);
        Savepoint spt1 = conn.setSavepoint("svpt1");
        stmt.execute(query1);
        ResultSet rs = stmt.executeQuery(query2);
        int no_of_rows = 0;

        while (rs.next()) {
            no_of_rows++;
        }
        assertEquals(1, no_of_rows);
        conn.releaseSavepoint(spt1);
        conn.commit();
        no_of_rows = 0;
        rs = stmt.executeQuery(query2);

        while (rs.next()) {
            no_of_rows++;
        }
        assertEquals(1, no_of_rows);
    }
}
