package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FloatParameter extends SimpleParameter<Float>{
    public FloatParameter() {
    }


    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setFloat(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setFloat(columnName,value);
        else load((PreparedStatement) callableStatement);
    }
}
