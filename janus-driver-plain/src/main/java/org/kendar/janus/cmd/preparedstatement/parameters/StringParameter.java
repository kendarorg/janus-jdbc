package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class StringParameter extends SimpleParameter<String> {
    public StringParameter(){

    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setString(columnName,value);
        else load((PreparedStatement) callableStatement);
    }
}
