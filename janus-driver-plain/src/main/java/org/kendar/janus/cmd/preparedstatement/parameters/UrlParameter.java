package org.kendar.janus.cmd.preparedstatement.parameters;

import java.net.URL;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UrlParameter extends SimpleParameter<URL> {
    public UrlParameter(){

    }
    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setURL(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setURL(columnName,value);
        else load((PreparedStatement) callableStatement);
    }
}
