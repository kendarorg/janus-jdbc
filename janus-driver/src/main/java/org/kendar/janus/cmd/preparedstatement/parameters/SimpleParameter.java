package org.kendar.janus.cmd.preparedstatement.parameters;

import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class SimpleParameter<T> implements PreparedStatementParameter {
    protected T value;
    protected int columnIndex;

    public SimpleParameter(){

    }

    public SimpleParameter(T value, int columnIndex){

        this.value = value;
        this.columnIndex = columnIndex;
    }
    @Override
    public abstract void load(PreparedStatement preparedStatement) throws SQLException;

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("columnIndex",columnIndex);
        builder.write("value",value);
    }

    @Override
    public Object deserialize(TypedSerializer input) {
        this.columnIndex = input.read("columnIndex");
        this.value = input.read("value");
        return this;
    }
}
