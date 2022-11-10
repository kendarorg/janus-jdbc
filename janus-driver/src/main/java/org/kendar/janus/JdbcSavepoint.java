package org.kendar.janus;

import java.sql.SQLException;
import java.sql.Savepoint;

public class JdbcSavepoint implements Savepoint {
    //TODO Implements
    @Override
    public int getSavepointId() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSavepointName() throws SQLException {
        throw new UnsupportedOperationException();
    }
}
