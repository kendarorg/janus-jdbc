/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.test.TestDb;
import org.h2.util.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test concurrent usage of the same connection.
 */
public class TestConcurrentConnectionUsageTest extends TestDb {

    @BeforeEach
    protected void beforeEach() throws SQLException {
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }


    @Test
    void testAutoCommit() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getConnection(getTestName());
        final PreparedStatement p1 = conn.prepareStatement("select 1 from dual");
        Task t = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    p1.executeQuery();
                    conn.setAutoCommit(true);
                    conn.setAutoCommit(false);
                }
            }
        }.execute();
        PreparedStatement prep = conn.prepareStatement("select ? from dual");
        for (int i = 0; i < 10; i++) {
            prep.setBinaryStream(1, new ByteArrayInputStream(new byte[1024]));
            prep.executeQuery();
        }
        t.get();
        conn.close();
    }

}
