package org.kendar.janus.cmd.preparedstatement.parameters;

import org.kendar.janus.types.JdbcArray;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ArrayParameter extends SimpleParameter<Array>{
    public ArrayParameter() {
    }

    public ArrayParameter withColumnName(String columnName){
        throw new UnsupportedOperationException("Can't assign column name to Array paramter");
    }

    @Override
    public ArrayParameter withValue(Array value){
        try {
            this.value =new JdbcArray().fromSqlArray(value);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setArray(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        this.load((PreparedStatement) callableStatement);
    }
}
