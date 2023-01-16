package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.CallableStatement;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NClobParameter extends SimpleParameter<NClob>{
    public NClobParameter() {
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setNClob(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setNClob(columnName,value);
        else load((PreparedStatement) callableStatement);
    }
}
