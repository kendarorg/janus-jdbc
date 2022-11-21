package org.kendar.janus.cmd.preparedstatement.parameters;

import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NullParameter implements PreparedStatementParameter {
    private int columnIndex;
    private String typeName;
    private Integer sqlType;
    private String columnName;



    public NullParameter withColumnIndex(int columnIndex){
        this.columnIndex = columnIndex;
        return this;
    }

    public NullParameter withColumnName(String columnName){
        this.columnName = columnName;
        return this;
    }

    public NullParameter withSqlType(int sqlType){
        this.sqlType = sqlType;
        return this;
    }

    public NullParameter(){

    }
    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        if(typeName==null) {
            preparedStatement.setNull(columnIndex, sqlType);
        }else{
            preparedStatement.setNull(columnIndex, sqlType,typeName);
        }
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(columnName!=null && !columnName.isEmpty()) {
            if(typeName==null) {
                callableStatement.setNull(columnName, sqlType);
            }else{
                callableStatement.setNull(columnName, sqlType,typeName);
            }
        }else{
            load((PreparedStatement) callableStatement);
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("columnIndex",columnIndex);
        builder.write("columnName",columnName);
        builder.write("sqlType",sqlType);
        builder.write("typeName",typeName);
    }

    @Override
    public Object deserialize(TypedSerializer builder) {
        columnIndex =builder.read("columnIndex");
        columnName =builder.read("columnName");
        sqlType =builder.read("sqlType");
        typeName =builder.read("typeName");
        return this;
    }

    @Override
    public String toString() {
        return "NullParameter{" +
                "\n\tcolumnIndex=" + columnIndex +
                "\n\tcolumnName=" + columnName +
                ", \n\ttypeName='" + typeName + '\'' +
                ", \n\tsqlType=" + sqlType +
                '}';
    }

    public PreparedStatementParameter withTypeName(String typeName) {
        this.typeName = typeName;
        return this;
    }
}
