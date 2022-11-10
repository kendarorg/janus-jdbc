package org.kendar.janus.cmd.preparedstatement.parameters;

import org.kendar.janus.types.JdbcArray;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ArrayParameter extends SimpleParameter<Array>{
    public ArrayParameter() {
    }

    public ArrayParameter(Array value, int columnIndex) throws SQLException {
        super(new JdbcArray().fromSqlArray(value), columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setArray(columnIndex,value);
    }
}
