package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ByteParameter extends SimpleParameter<Byte>{
    public ByteParameter() {
    }

    public ByteParameter(byte value, int columnIndex) {
        super(value, columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setByte(columnIndex,value);
    }
}
