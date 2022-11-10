package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FloatParameter extends SimpleParameter<Float>{
    public FloatParameter() {
    }

    public FloatParameter(float value, int columnIndex) {
        super(value, columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setFloat(columnIndex,value);
    }
}
