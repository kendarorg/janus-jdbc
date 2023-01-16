package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class ShortParameter extends SimpleParameter<Short>{
    public ShortParameter() {
    }


    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setShort(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setShort(columnName,value);
        else load((PreparedStatement) callableStatement);
    }

}
