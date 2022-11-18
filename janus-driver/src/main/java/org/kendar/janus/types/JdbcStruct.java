package org.kendar.janus.types;

import org.kendar.janus.serialization.TypedSerializable;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

public class JdbcStruct implements Struct, TypedSerializable {
    private String sqlTypeName;
    private Object[] attributes;



    public JdbcStruct() {

    }

    public JdbcStruct fromData(String sqlTypeName,Object[] attributes){

        this.sqlTypeName = sqlTypeName;
        this.attributes = attributes;
        return this;
    }

    public JdbcStruct fromStruct(Struct struct) throws SQLException {
        sqlTypeName = struct.getSQLTypeName();
        attributes = struct.getAttributes();
        return this;
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return sqlTypeName;
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        //FIXME
        return new Object[0];
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("sqlTypeName",sqlTypeName);
        builder.write("attributes",attributes);
    }

    @Override
    public Object deserialize(TypedSerializer builder) {
        sqlTypeName = builder.read("sqlTypeName");
        attributes = builder.read("attributes");
        return this;
    }
}
