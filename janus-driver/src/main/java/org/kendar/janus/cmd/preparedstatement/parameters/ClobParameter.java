package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClobParameter extends SimpleParameter<Clob>{
    public ClobParameter() {
    }

    public ClobParameter(Clob value, int columnIndex) {
        super(value, columnIndex);
    }
    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setClob(columnIndex,value);
    }
}
