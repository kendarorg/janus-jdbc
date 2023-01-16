package org.kendar.janus.types;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcType {
    Object toNativeObject(Connection connection) throws SQLException;
}
