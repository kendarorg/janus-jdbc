package org.kendar.janus.types;

import org.apache.commons.lang3.ClassUtils;
import org.kendar.janus.serialization.TypedSerializable;
import org.kendar.janus.serialization.TypedSerializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.RowId;
import java.sql.SQLException;

public class JdbcRowId  implements TypedSerializable, RowId,JdbcType {

    public Object getOriginalValue() {
        return originalValue;
    }

    private Object originalValue;
    private String string;
    private int hashCode;
    private byte[] data;

    public JdbcRowId(){

    }


    public JdbcRowId fromObject(Object rowId){

        this.string = rowId.toString();
        this.hashCode = rowId.hashCode();
        var clazz = rowId.getClass();
        this.originalValue = rowId;

        if(rowId instanceof RowId){
            this.data = ((RowId)rowId).getBytes();
            originalValue=null;
        }else if(ClassUtils.isAssignable(clazz,long.class)){
            this.data = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong((long)rowId).array();
        }else if(ClassUtils.isAssignable(clazz,byte[].class)){
            this.data = (byte[])rowId;
        }else if(ClassUtils.isAssignable(clazz,String.class)){
            this.data = ((String)rowId).getBytes(StandardCharsets.UTF_8);
        }
        return this;
    }


    @Override
    public byte[] getBytes() {
        return data;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("data",data);
        builder.write("originalValue",originalValue);
        builder.write("string",string);
        builder.write("hashCode",hashCode);
    }

    @Override
    public Object deserialize(TypedSerializer builder) {
        data = builder.read("data");
        originalValue = builder.read("originalValue");
        string = builder.read("string");
        hashCode = builder.read("hashCode");
        return this;
    }


    @Override
    public Object toNativeObject(Connection connection) throws SQLException {
        return this;
    }
}
