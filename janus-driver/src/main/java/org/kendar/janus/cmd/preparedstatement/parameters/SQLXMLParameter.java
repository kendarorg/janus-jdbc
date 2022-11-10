package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;

public class SQLXMLParameter extends SimpleParameter<SQLXML>{
    public SQLXMLParameter() {
    }

    public SQLXMLParameter(SQLXML value, int columnIndex) {
        super(value, columnIndex);
    }
    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setSQLXML(columnIndex,value);
    }
}
