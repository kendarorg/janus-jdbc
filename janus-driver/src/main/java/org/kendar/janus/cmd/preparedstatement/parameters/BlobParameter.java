package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BlobParameter extends SimpleParameter<Blob>{
    public BlobParameter() {
    }

    public BlobParameter(Blob value, int columnIndex) {
        super(value, columnIndex);
    }
    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setBlob(columnIndex,value);
    }
}
