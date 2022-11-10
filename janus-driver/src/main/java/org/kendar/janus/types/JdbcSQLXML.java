package org.kendar.janus.types;

import org.xml.sax.InputSource;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import java.io.*;
import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.sql.SQLXML;

public class JdbcSQLXML extends BigFieldBase<String,JdbcSQLXML, SQLXML, String> implements SQLXML{
    @Override
    protected String sqlObjectToDataArray(SQLXML input, long length) throws SQLException, IOException {
        return input.getString();
    }

    @Override
    protected String sourceToDataArray(String input, long length) throws IOException {
        return input;
    }

    @Override
    protected void freeSqlObject(SQLXML input) throws SQLException {
        input.free();
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(this.data.getBytes());
    }

    @Override
    public String getString() throws SQLException {
        return this.data;
    }

    @Override
    public void setString(String value) throws SQLException {
        this.data = value;
    }

    @Override
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        try {
            final Constructor<T> constructor = sourceClass.getConstructor(InputSource.class);
            return constructor.newInstance(new InputSource(this.getCharacterStream()));
        }
        catch (Exception ex) {
            return null;
        }
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return new StringReader(this.data);
    }

    @Override
    public OutputStream setBinaryStream() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writer setCharacterStream() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
