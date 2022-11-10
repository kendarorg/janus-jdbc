package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StringParameter extends SimpleParameter<String> {
    public StringParameter(){

    }
    public StringParameter(String x, int parameterIndex) {
        super(x,parameterIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(columnIndex,value);
    }
}
