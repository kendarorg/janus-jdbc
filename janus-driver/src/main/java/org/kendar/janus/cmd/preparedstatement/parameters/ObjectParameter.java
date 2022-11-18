package org.kendar.janus.cmd.preparedstatement.parameters;

import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ObjectParameter implements PreparedStatementParameter {
    private Object value;
    private int columnIndex;
    private Integer targetSqlType;
    private Integer scaleOrLength;
    private String columnName;
    private boolean out;

    private String typeName;


    public ObjectParameter withTargetSqlType(int targetSqlType){

        this.targetSqlType = targetSqlType;
        return this;
    }

    public ObjectParameter withTypeName(String typeName){

        this.typeName = typeName;
        return this;
    }

    public ObjectParameter withScaleOrLength(int scaleOrLength){

        this.targetSqlType = scaleOrLength;
        return this;
    }

    public ObjectParameter withColumnIndex(int columnIndex){
        this.columnIndex = columnIndex;
        return this;
    }

    public ObjectParameter withColumnName(String columnName){
        this.columnName = columnName;
        return this;
    }

    public ObjectParameter withValue(Object value){
        this.value = value;
        return this;
    }

    public ObjectParameter asOutParameter(){
        this.out= true;
        return this;
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        if(targetSqlType==null) {
            preparedStatement.setObject(columnIndex,value);
        }else if(scaleOrLength==null){
            preparedStatement.setObject(columnIndex,value,targetSqlType);
        }else{
            preparedStatement.setObject(columnIndex,value,targetSqlType,scaleOrLength);
        }
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(columnName!=null && !columnName.isEmpty()) {
            if (targetSqlType == null) {
                callableStatement.setObject(columnName, value);
            } else if (scaleOrLength == null) {
                callableStatement.setObject(columnName, value, targetSqlType);
            } else {
                callableStatement.setObject(columnName, value, targetSqlType, scaleOrLength);
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


    public boolean isOut() {
        return out;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("value",value);
        builder.write("columnIndex",columnIndex);
        builder.write("targetSqlType",targetSqlType);
        builder.write("scaleOrLength",scaleOrLength);
        builder.write("columnName",columnName);
        builder.write("typeName",typeName);
        builder.write("out",out);
    }

    @Override
    public Object deserialize(TypedSerializer builder) {
        value =builder.read("value");
        columnIndex =builder.read("columnIndex");
        targetSqlType =builder.read("targetSqlType");
        scaleOrLength =builder.read("scaleOrLength");
        columnName =builder.read("columnName");
        typeName =builder.read("typeName");
        out =builder.read("out");
        return this;
    }

    @Override
    public String toString() {
        return "ObjectParameter{" +
                "\n\tvalue=" + value +
                ", \n\tcolumnIndex=" + columnIndex +
                ", \n\tcolumnName=" + columnName +
                ", \n\tout=" + out +
                ", \n\ttargetSqlType=" + targetSqlType +
                ", \n\tscaleOrLength=" + scaleOrLength +
                '}';
    }
}
