package org.kendar.janus.cmd.preparedstatement.parameters;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BigDecimalParameter extends SimpleParameter<BigDecimal>{
    public BigDecimalParameter() {
    }


    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setBigDecimal(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setBigDecimal(columnName,value);
        else load((PreparedStatement) callableStatement);
    }
}
