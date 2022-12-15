package org.kendar.janus.cmd.preparedstatement;

import org.kendar.janus.serialization.TypedSerializable;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

public interface PreparedStatementParameter extends TypedSerializable {
    void load(PreparedStatement preparedStatement) throws SQLException;
    void load(CallableStatement callableStatement) throws SQLException;
    int getColumnIndex();
    String getColumnName();

    Object getValue();
}
