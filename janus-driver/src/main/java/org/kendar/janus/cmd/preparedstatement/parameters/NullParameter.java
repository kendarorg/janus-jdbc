package org.kendar.janus.cmd.preparedstatement.parameters;

import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NullParameter implements PreparedStatementParameter {
    private int columnIndex;
    private String typeName;
    private Integer sqlType;

    public NullParameter(){

    }

    public NullParameter(int columnIndex,int sqlType,String typeName){

        this.sqlType = sqlType==Integer.MAX_VALUE?null:sqlType;
        this.columnIndex = columnIndex;
        this.typeName = typeName;
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
    public void serialize(TypedSerializer builder) {
        builder.write("columnIndex",columnIndex);
        builder.write("sqlType",sqlType);
        builder.write("typeName",typeName);
    }

    @Override
    public Object deserialize(TypedSerializer builder) {
        columnIndex =builder.read("columnIndex");
        sqlType =builder.read("sqlType");
        typeName =builder.read("typeName");
        return this;
    }

    @Override
    public String toString() {
        return "NullParameter{" +
                "\n\tcolumnIndex=" + columnIndex +
                ", \n\ttypeName='" + typeName + '\'' +
                ", \n\tsqlType=" + sqlType +
                '}';
    }
}
