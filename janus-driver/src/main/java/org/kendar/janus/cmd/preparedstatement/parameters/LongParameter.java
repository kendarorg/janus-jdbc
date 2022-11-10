package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LongParameter extends SimpleParameter<Long>{
    public LongParameter() {
    }

    public LongParameter(long value, int columnIndex) {
        super(value, columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setLong(columnIndex,value);
    }
}
