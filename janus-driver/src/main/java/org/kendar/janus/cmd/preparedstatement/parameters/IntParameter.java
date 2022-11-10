package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class IntParameter extends SimpleParameter<Integer>{
    public IntParameter() {
    }

    public IntParameter(int value, int columnIndex) {
        super(value, columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(columnIndex,value);
    }
}
