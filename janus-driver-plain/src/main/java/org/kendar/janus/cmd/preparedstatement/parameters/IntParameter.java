package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class IntParameter extends SimpleParameter<Integer>{
    public IntParameter() {
    }


    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setInt(columnName,value);
        else load((PreparedStatement) callableStatement);
    }
}
