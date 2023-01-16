package org.kendar.janus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.janus.utils.TestBase;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("resource")
public class MixedStatementsTest extends TestBase {

    @BeforeEach
    protected void beforeEach() throws SQLException {
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }
    @Test
    public void testMixedStatements() throws IOException, InterruptedException, SQLException, ClassNotFoundException {

        createFooTableWithField("testMixedStatements");

        var conn = driver.connect(CONNECT_URL,null);
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT foo FROM bar where foo='testMixedStatements'");
        Assertions.assertNotNull(rs);

        // or alternatively, if you don't know ahead of time that
        // the query will be a SELECT...

        if (stmt.execute("SELECT foo FROM bar where foo='testMixedStatements'")) {
            rs = stmt.getResultSet();
        }
        Assertions.assertNotNull(rs);
        while(rs.next()){
            assertEquals("testMixedStatements",rs.getString(1));
        }

    }

    @Test
    public void testMixedStatementsConflicting() throws Exception {

        String createStatement = "create table if not exists mixConflict  (foo varchar(50))";
        var dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        dbConnection.createStatement().executeUpdate(createStatement);
        dbConnection.createStatement().execute("INSERT INTO mixConflict (foo) VALUES('a')");
        dbConnection.createStatement().execute("INSERT INTO mixConflict (foo) VALUES('b')");

        var conn = driver.connect(CONNECT_URL,null);
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT foo FROM mixConflict ORDER BY foo ASC");
        Assertions.assertNotNull(rs);
        if(rs.next()){
            assertEquals("a",rs.getString(1));
        }else{
            throw new Exception("Error on rs.next a");
        }

        var stmt2 = conn.createStatement();
        var rs2 = stmt2.executeQuery("SELECT foo FROM mixConflict ORDER BY foo DESC");
        Assertions.assertNotNull(rs2);
        if(rs2.next()){
            //FAIL IF NOT REENTRANT with "a"
            assertEquals("b",rs2.getString(1));
        }else{
            throw new Exception("Error on rs2.next b");
        }

        if(rs.next()){
            assertEquals("b",rs.getString(1));
        }else{
            throw new Exception("Error on rs.next b");
        }


        if(rs2.next()){
            assertEquals("a",rs2.getString(1));
        }else{
            throw new Exception("Error on rs2.next a");
        }

    }
}
