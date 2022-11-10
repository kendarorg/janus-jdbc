package org.kendar.janus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.janus.utils.TestBase;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConnectionTest extends TestBase {

    @BeforeEach
    protected void beforeEach(){
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }

    @Test
    void createBlob() throws SQLException {
        var conn = driver.connect(CONNECT_URL,null);
        var blob = conn.createBlob();
        assertNotNull(blob);
    }
}
