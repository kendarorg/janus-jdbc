package org.kendar.janus.types;

import org.kendar.janus.serialization.TypedSerializable;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.utils.JdbcArrayTypeTranslator;
import org.kendar.janus.utils.SqlTypes;

import java.sql.*;
import java.util.Map;

public class JdbcArray implements Array,TypedSerializable,JdbcType {
    private String baseTypeName;
    private int baseType;
    private Object array;

    public JdbcArray(){

    }
    public JdbcArray(String baseTypeName, Object[] array) {
        this.baseTypeName = baseTypeName;
        this.baseType = JdbcArrayTypeTranslator.translateToBaseType(baseTypeName);
        this.array = array;
    }

    public JdbcArray fromJavaArray(String typeName,Object[] elements){
        this.baseTypeName = typeName;
        this.baseType = JdbcArrayTypeTranslator.translateToBaseType(typeName);
        this.array = elements;
        return this;
    }

    public JdbcArray fromSqlArray(Array array) throws SQLException {
        this.baseTypeName = array.getBaseTypeName();
        this.baseType = array.getBaseType();
        if (SqlTypes.STRUCT.is(baseType)) {
            final Object[] original = (Object[])array.getArray();
            final Struct[] copy = new JdbcStruct[original.length];
            for (int i = 0; i < original.length; ++i) {
                copy[i] = new JdbcStruct().fromStruct((Struct)original[i]);
            }
            this.array = copy;
        }else{
            this.array = array.getArray();
        }
        array.free();
        return this;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("baseType", baseType);
        builder.write("baseTypeName", baseTypeName);
        builder.write("array", array);
    }

    @Override
    public Object deserialize(TypedSerializer builder) {
        baseType = builder.read("baseType");
        baseTypeName = builder.read("baseTypeName");
        array = builder.read("array");
        return this;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return baseTypeName;
    }

    @Override
    public int getBaseType() throws SQLException {
        return baseType;
    }

    @Override
    public Object getArray() throws SQLException {
        return array;
    }

    //TODO Implement

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return null;
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public void free() throws SQLException {
        this.array = null;
    }

    @Override
    public Object toNativeObject(Connection connection) throws SQLException {
        return connection.createArrayOf(baseTypeName,(Object[])array);
    }
}
