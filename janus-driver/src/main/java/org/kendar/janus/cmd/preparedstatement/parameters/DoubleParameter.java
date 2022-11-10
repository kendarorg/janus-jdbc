package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DoubleParameter extends SimpleParameter<Double>{
    public DoubleParameter() {
    }

    public DoubleParameter(double value, int columnIndex) {
        super(value, columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setDouble(columnIndex,value);
    }
}
