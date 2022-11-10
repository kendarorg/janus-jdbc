package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ShortParameter extends SimpleParameter<Short>{
    public ShortParameter() {
    }

    public ShortParameter(short value, int columnIndex) {
        super(value, columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setShort(columnIndex,value);
    }
}
