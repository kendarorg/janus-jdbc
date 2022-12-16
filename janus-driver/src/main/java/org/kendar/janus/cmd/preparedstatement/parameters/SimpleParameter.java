package org.kendar.janus.cmd.preparedstatement.parameters;

import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class SimpleParameter<T> implements PreparedStatementParameter {
    protected String columnName;
    protected T value;
    protected int columnIndex;

    public Object getValue(){
        return value;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }


    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public SimpleParameter(){

    }

    public SimpleParameter<T> withColumnIndex(int columnIndex){
        this.columnIndex = columnIndex;
        return this;
    }

    public SimpleParameter<T> withColumnName(String columnName){
        this.columnName = columnName;
        return this;
    }

    public SimpleParameter<T> withValue(T value){
        this.value = value;
        return this;
    }

    @Override
    public abstract void load(PreparedStatement preparedStatement) throws SQLException;
    @Override
    public abstract void load(CallableStatement callableStatement) throws SQLException;

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("columnIndex",columnIndex);
        builder.write("columnName",columnName);
        builder.write("value",value);
    }

    @Override
    public Object deserialize(TypedSerializer input) {
        this.columnIndex = input.read("columnIndex");
        this.columnName = input.read("columnName");
        this.value = input.read("value");
        return this;
    }

    protected boolean hasColumnName(){
        return columnName!=null && !columnName.isEmpty();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+ "{" +
                "\n\tvalue=" + value +
                ", \n\tcolumnIndex=" + columnIndex +
                ", \n\tcolumnName=" + columnName +
                '}';
    }
}
