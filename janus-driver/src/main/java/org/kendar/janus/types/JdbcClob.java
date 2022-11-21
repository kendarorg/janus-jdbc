package org.kendar.janus.types;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;

public class JdbcClob extends BigFieldBase<char[],JdbcClob, Clob, Reader> implements Clob,JdbcType{
    public JdbcClob(){

    }

    @Override
    protected char[] sqlObjectToDataArray(Clob input, long length) throws SQLException, IOException {
        return sourceToDataArray(input.getCharacterStream(),length);
    }

    @Override
    protected char[] sourceToDataArray(Reader input, long length) throws IOException {
        final StringBuilder sw = new StringBuilder();
        final char[] buff = new char[1024];
        int len;
        if(length==-1) {
            while ((len = input.read(buff)) > 0) {
                sw.append(buff, 0, len);
            }
        }else{
            for (long toRead = length; toRead > 0L && (len = input.read(buff, 0, (int)((toRead > 1024L) ? 1024L : toRead))) > 0; toRead -= len) {
                sw.append(buff, 0, len);
            }
        }
        return sw.toString().toCharArray();
    }

    @Override
    protected void freeSqlObject(Clob input) throws SQLException {
        input.free();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        return new String(this.data, (int)pos - 1, length);
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return new StringReader(new String(this.data));
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        return new ByteArrayInputStream(new String(this.data).getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new CharArrayReader(this.data, (int)pos, (int)length);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        return setString(pos, str, 0, str == null ? 0
                : str.length());
    }

    @Override
    public void truncate(long len) throws SQLException {
        char[] newData = new char[(int) len];

        System.arraycopy(data, 0, newData, 0, (int) len);
        data = newData;
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        return new ByteArrayOutputStream() {

            boolean closed = false;

            public synchronized void close() throws IOException {

                if (closed) {
                    return;
                }

                closed = true;

                final byte[] bytes  = super.buf;
                final int    length = super.count;

                super.buf   = null;
                super.count = 0;

                try {
                    final String str = new String(bytes, 0, length,
                            "US-ASCII");

                    data = str.toCharArray();
                } catch (Throwable e) {
                    throw new IOException(e);
                }
            }
        };
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        pos = pos-1;
        var chars = str.toCharArray();
        if(data==null){
            data = new char[0];
        }

        var minSize =(int) (pos+len);
        if(data.length<minSize){
            var tmp = new char[minSize];
            System.arraycopy(data,0,tmp,0,data.length);
            data = tmp;
        }
        System.arraycopy(chars,offset,data,(int)pos,len);
        length = data.length;
        return len;
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        throw new UnsupportedOperationException();
    }


    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object toNativeObject(Connection connection) throws SQLException {
        var result = connection.createClob();
        try {
            result.setCharacterStream(0).write(data);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return result;
    }
}
