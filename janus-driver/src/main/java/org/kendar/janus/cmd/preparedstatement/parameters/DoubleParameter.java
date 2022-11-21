package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DoubleParameter extends SimpleParameter<Double>{
    public DoubleParameter() {
    }


    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setDouble(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setDouble(columnName,value);
        else load((PreparedStatement) callableStatement);
    }
}
