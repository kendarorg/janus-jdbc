package org.kendar.janus.cmd.preparedstatement.parameters;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BigDecimalParameter extends SimpleParameter<BigDecimal>{
    public BigDecimalParameter() {
    }

    public BigDecimalParameter(BigDecimal value, int columnIndex) {
        super(value, columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setBigDecimal(columnIndex,value);
    }
}
