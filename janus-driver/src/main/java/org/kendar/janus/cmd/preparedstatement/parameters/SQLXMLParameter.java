package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;

public class SQLXMLParameter extends SimpleParameter<SQLXML>{
    public SQLXMLParameter() {
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setSQLXML(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setSQLXML(columnName,value);
        else load((PreparedStatement) callableStatement);
    }
}
