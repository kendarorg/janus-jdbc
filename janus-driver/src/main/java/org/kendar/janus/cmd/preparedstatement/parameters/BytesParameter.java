package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BytesParameter extends SimpleParameter<byte[]>{
    public BytesParameter() {
    }

    public BytesParameter(byte[] value, int columnIndex) {
        super(value, columnIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setBytes(columnIndex,value);
    }
}
