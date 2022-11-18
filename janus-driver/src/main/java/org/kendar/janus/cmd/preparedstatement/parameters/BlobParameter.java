package org.kendar.janus.cmd.preparedstatement.parameters;

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BlobParameter extends SimpleParameter<Blob>{
    public BlobParameter() {
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setBlob(columnIndex,value);
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setBlob(columnName,value);
        else load((PreparedStatement) callableStatement);
    }
}
