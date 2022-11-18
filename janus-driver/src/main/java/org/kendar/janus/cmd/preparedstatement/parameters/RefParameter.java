package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.SQLException;

public class RefParameter extends SimpleParameter<Ref>{
    public RefParameter() {
    }

    public RefParameter withColumnName(String columnName){
        throw new UnsupportedOperationException("Can't assign column name to Array paramter");
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setRef(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        throw new UnsupportedOperationException("No named params for ref");
    }
}
