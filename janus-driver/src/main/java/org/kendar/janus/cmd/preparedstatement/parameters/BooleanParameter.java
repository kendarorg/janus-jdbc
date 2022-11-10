package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BooleanParameter extends SimpleParameter<Boolean>{
    public BooleanParameter() {
    }

    public BooleanParameter(boolean value, int columnIndex) {
        super(value, columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setBoolean(columnIndex,value);
    }
}
