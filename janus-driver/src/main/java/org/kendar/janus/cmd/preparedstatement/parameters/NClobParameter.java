package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NClobParameter extends SimpleParameter<NClob>{
    public NClobParameter() {
    }

    public NClobParameter(NClob value, int columnIndex) {
        super(value, columnIndex);
    }
    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setNClob(columnIndex,value);
    }
}
