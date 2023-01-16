package org.kendar.janus.cmd.preparedstatement.parameters;

import org.apache.commons.lang3.ClassUtils;
import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.types.JdbcBlob;
import org.kendar.janus.types.JdbcNClob;
import org.kendar.janus.types.JdbcType;

import java.io.*;
import java.sql.*;

public class ObjectParameter implements PreparedStatementParameter {
    private Object value;
    private int columnIndex;
    private Integer targetSqlType;
    private Integer scaleOrLength;
    private String columnName;
    private boolean out;

    private String typeName;
    private boolean serializable=false;


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

    public ObjectParameter withValue(Object value) throws SQLException {
        if(value==null){
            return this;
        }
        var objClass =value.getClass();
        if(ClassUtils.isPrimitiveOrWrapper(objClass)) {
            this.value = value;
        }else if(ClassUtils.isAssignable(objClass, JdbcType.class)) {
            this.value = value;
        }else if(ClassUtils.isAssignable(objClass, InputStream.class)) {
            var jdbcBlob = new JdbcBlob().fromSource((InputStream)value);
            this.value = jdbcBlob;
        }else if(ClassUtils.isAssignable(objClass, Reader.class)) {
            var jdbcBlob = new JdbcNClob().fromSource((Reader)value);
            this.value = jdbcBlob;
        }else{
            try {
                var byteArrayOutputStream
                        = new ByteArrayOutputStream();
                var objectOutputStream
                        = new ObjectOutputStream(byteArrayOutputStream);
                objectOutputStream.writeObject(value);
                objectOutputStream.flush();
                objectOutputStream.close();
                this.value = byteArrayOutputStream.toByteArray();
                serializable = true;
            }catch(Exception ex){
                throw new SQLException(ex);
            }
        }
        return this;
    }

    public ObjectParameter asOutParameter(){
        this.out= true;
        return this;
    }

    private Object toLocalObject(Statement statement) throws SQLException {
        if(serializable){
            try {
                var byteArrayOutputStream
                        = new ByteArrayInputStream((byte[])value);
                var objectOutputStream
                        = new ObjectInputStream(byteArrayOutputStream);
                return objectOutputStream.readObject();
            }catch(Exception ex){
                throw new SQLException(ex);
            }
        }
        if(value==null){
            return null;
        }
        var objClass = value.getClass();
        if(ClassUtils.isPrimitiveOrWrapper(objClass)) {
            return value;
        }else if(ClassUtils.isAssignable(objClass, JdbcType.class)) {
            return ((JdbcType)value).toNativeObject(statement.getConnection());
        }
        throw new SQLException("Inconvertible type "+objClass.getName());
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {
        var real = toLocalObject(preparedStatement);
        if(targetSqlType==null) {
            preparedStatement.setObject(columnIndex,real);
        }else if(scaleOrLength==null){
            preparedStatement.setObject(columnIndex,real,targetSqlType);
        }else{
            preparedStatement.setObject(columnIndex,real,targetSqlType,scaleOrLength);
        }
    }

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(columnName!=null && !columnName.isEmpty()) {
            var real = toLocalObject(callableStatement);
            if (targetSqlType == null) {
                callableStatement.setObject(columnName, real);
            } else if (scaleOrLength == null) {
                callableStatement.setObject(columnName, real, targetSqlType);
            } else {
                callableStatement.setObject(columnName, real, targetSqlType, scaleOrLength);
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
    public Object getValue() {
        return value;
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
        builder.write("serializable",serializable);
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
        serializable =builder.read("serializable");
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

    public void loadOut(CallableStatement callableStatement) throws SQLException {
        if(columnName!=null && !columnName.isEmpty()) {
            if(scaleOrLength!=null){
                callableStatement.registerOutParameter(columnName,targetSqlType,scaleOrLength);
            }else if(typeName!=null){
                callableStatement.registerOutParameter(columnName,targetSqlType,typeName);
            }else{
                callableStatement.registerOutParameter(columnName,targetSqlType);
            }
        }else{
            if(scaleOrLength!=null){
                callableStatement.registerOutParameter(columnIndex,targetSqlType,scaleOrLength);
            }else if(typeName!=null){
                callableStatement.registerOutParameter(columnIndex,targetSqlType,typeName);
            }else{
                callableStatement.registerOutParameter(columnIndex,targetSqlType);
            }
        }
    }
}
