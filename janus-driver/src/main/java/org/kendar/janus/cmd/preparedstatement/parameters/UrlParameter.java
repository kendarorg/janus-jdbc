package org.kendar.janus.cmd.preparedstatement.parameters;

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UrlParameter extends SimpleParameter<URL> {
    public UrlParameter(){

    }
    public UrlParameter(URL x, int parameterIndex) {
        super(x,parameterIndex);
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setURL(columnIndex,value);
    }
}
