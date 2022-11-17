package org.kendar.janus.cmd.preparedstatement.parameters;

import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ObjectParameter implements PreparedStatementParameter {
    private Object value;
    private int columnIndex;
    private int targetSqlType;
    private int scaleOrLength;

    public ObjectParameter(Object value, int columnIndex, int targetSqlType, int scaleOrLength) {
        this.value = value;
        this.columnIndex = columnIndex;
        this.targetSqlType = targetSqlType;
        this.scaleOrLength = scaleOrLength;
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        if(targetSqlType==Integer.MAX_VALUE) {
            preparedStatement.setObject(columnIndex,value);
        }else if(scaleOrLength==Integer.MAX_VALUE){
            preparedStatement.setObject(columnIndex,value,targetSqlType);
        }else{
            preparedStatement.setObject(columnIndex,value,targetSqlType,scaleOrLength);
        }
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("value",value);
        builder.write("columnIndex",columnIndex);
        builder.write("targetSqlType",targetSqlType);
        builder.write("scaleOrLength",scaleOrLength);
    }

    @Override
    public Object deserialize(TypedSerializer builder) {
        value =builder.read("value");
        columnIndex =builder.read("columnIndex");
        targetSqlType =builder.read("targetSqlType");
        scaleOrLength =builder.read("scaleOrLength");
        return this;
    }

    @Override
    public String toString() {
        return "ObjectParameter{" +
                "\n\tvalue=" + value +
                ", \n\tcolumnIndex=" + columnIndex +
                ", \n\ttargetSqlType=" + targetSqlType +
                ", \n\tscaleOrLength=" + scaleOrLength +
                '}';
    }
}
