package org.kendar.janus.types;

import org.kendar.janus.serialization.TypedSerializable;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.utils.ExceptionsWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.sql.SQLException;

@SuppressWarnings("ThrowableNotThrown")
public abstract class BigFieldBase<T,K,J,S> implements TypedSerializable {
    public T getData() {
        return data;
    }

    public long getLength() {
        return length;
    }

    protected T data;
    protected long length;

    protected abstract T sqlObjectToDataArray(J input, long length) throws SQLException, IOException;
    protected abstract T sourceToDataArray(S input, long length) throws IOException;
    protected abstract void freeSqlObject(J input) throws SQLException;


    public K withData(T data){
        this.data = data;
        return (K)this;
    }
    public K withLength(long length){
        this.length = length;
        return (K)this;
    }

    public K fromSqlType(J blob) throws SQLException {

        InputStream is = null;
        try {
            if(blob == null){
                this.data=null;
                this.length=0;
            }else {
                this.data = sqlObjectToDataArray(blob, -1);
                this.length = Array.getLength(data);
                freeSqlObject(blob);
            }
        }
        catch (Exception e) {
            ExceptionsWrapper.toSQLException(e);
        }
        return (K)this;
    }

    public K fromSource(S input) throws SQLException {
        try {
            if(input == null){
                this.data=null;
                this.length=0;
            }else {
                this.data = sourceToDataArray(input, -1);
                this.length = Array.getLength(data);
            }
        }
        catch (Exception e) {
            ExceptionsWrapper.toSQLException(e);
        }
        return (K)this;
    }

    public K fromSource(S input,long length) throws SQLException {
        try {
            if(input == null){
                this.data=null;
                this.length=0;
            }else {
                this.data = sourceToDataArray(input, length);
                this.length = Array.getLength(data);
            }
        }
        catch (Exception e) {
            ExceptionsWrapper.toSQLException(e);
        }
        return (K)this;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("length",length);
        builder.write("data",data);
    }

    @Override
    public Object deserialize(TypedSerializer builder) {
        length = builder.read("length");
        data = builder.read("data");
        return this;
    }

    public long length() throws SQLException {
        return length;
    }
    public void free() throws SQLException {
        this.data = null;
        this.length=0;
    }
}
