package org.kendar;

import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

public class ExternalTest {
    @Test
    void doTest() throws SQLException, ClassNotFoundException {
        Class.forName("org.kendar.janus.JdbcDriver");
        var connString = "jdbc:janus:http://127.0.0.1/api/db/be";
        var mainConn = DriverManager.getConnection(connString);
        mainConn.createStatement().execute("create table if not exists " +
                "persons(" +
                "id IDENTITY NOT NULL PRIMARY KEY, " +
                "name varchar(255));");
    }
}
