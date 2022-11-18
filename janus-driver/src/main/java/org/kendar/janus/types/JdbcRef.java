package org.kendar.janus.types;

import org.kendar.janus.serialization.TypedSerializable;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.Ref;
import java.sql.SQLException;
import java.util.Map;

public class JdbcRef implements Ref, TypedSerializable {

    private String baseTypeName;

    public Object getJavaObject() {
        return javaObject;
    }

    public void setJavaObject(Object javaObject) {
        this.javaObject = javaObject;
    }

    private Object javaObject;
    @Override
    public String getBaseTypeName() throws SQLException {
        return baseTypeName;
    }

    @Override
    public Object getObject(Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Object getObject() throws SQLException {
        return javaObject;
    }

    @Override
    public void setObject(Object value) throws SQLException {
        javaObject = value;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("javaObject",javaObject);
        builder.write("baseTypeName",baseTypeName);
    }

    @Override
    public Object deserialize(TypedSerializer builder) {
        javaObject = builder.read("javaObject");
        baseTypeName = builder.read("baseTypeName");
        return this;
    }
}
