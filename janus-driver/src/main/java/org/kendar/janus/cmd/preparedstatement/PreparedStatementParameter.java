package org.kendar.janus.cmd.preparedstatement;

import org.kendar.janus.serialization.TypedSerializable;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementParameter extends TypedSerializable {
    void load(PreparedStatement preparedStatement) throws SQLException;
}
