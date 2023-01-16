/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import org.h2.test.TestDb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

/**
 * Tests the server by creating many JDBC objects (result sets and so on).
 */
public class TestManyJdbcObjectsTest extends TestDb {

    @BeforeEach
    protected void beforeEach() throws SQLException {
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }

    @Test
    public void test() throws SQLException {
        testNestedResultSets();
        testManyConnections();
        testOneConnectionPrepare();
        deleteDb("manyObjects");
    }

    private void testNestedResultSets() throws SQLException {

        deleteDb("manyObjects");
        Connection conn = getConnection("manyObjects");
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rsTables = meta.getColumns(null, null, null, null);
        while (rsTables.next()) {
            meta.getExportedKeys(null, null, "TEST");
            meta.getImportedKeys(null, null, "TEST");
        }
        conn.close();
    }

    private void testManyConnections() throws SQLException {

        // SERVER_CACHED_OBJECTS = 1000: connections = 20 (1250)
        // SERVER_CACHED_OBJECTS = 500: connections = 40
        // SERVER_CACHED_OBJECTS = 50: connections = 120
        deleteDb("manyObjects");
        //int connCount = getSize(4, 40);
        int connCount = 10;
        Connection[] conn = new Connection[connCount];
        for (int i = 0; i < connCount; i++) {
            conn[i] = getConnection("manyObjects");
        }
        //int len = getSize(50, 500);
        int len = 10;
        for (int j = 0; j < len; j++) {
            if ((j % 10) == 0) {
                trace("j=" + j);
            }
            for (int i = 0; i < connCount; i++) {
                conn[i].getMetaData().getSchemas().close();
            }
        }
        for (int i = 0; i < connCount; i++) {
            conn[i].close();
        }
    }

    private void testOneConnectionPrepare() throws SQLException {
        deleteDb("manyObjects");
        Connection conn = getConnection("manyObjects");
        PreparedStatement prep;
        Statement stat;
        int size = 10;//getSize(10, 1000);
        for (int i = 0; i < size; i++) {
            conn.getMetaData();
        }
        for (int i = 0; i < size; i++) {
            conn.createStatement();
        }
        stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        for (int i = 0; i < size; i++) {
            stat.executeQuery("SELECT * FROM TEST WHERE 1=0");
        }
        for (int i = 0; i < size; i++) {
            stat.executeQuery("SELECT * FROM TEST");
        }
        for (int i = 0; i < size; i++) {
            conn.prepareStatement("SELECT * FROM TEST");
        }
        prep = conn.prepareStatement("SELECT * FROM TEST WHERE 1=0");
        for (int i = 0; i < size; i++) {
            prep.executeQuery();
        }
        prep = conn.prepareStatement("SELECT * FROM TEST");
        for (int i = 0; i < size; i++) {
            prep.executeQuery();
        }
        conn.close();
    }

}
