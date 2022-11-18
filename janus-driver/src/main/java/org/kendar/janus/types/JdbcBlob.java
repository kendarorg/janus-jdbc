package org.kendar.janus.types;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.lang.reflect.Array;
import java.sql.Blob;
import java.sql.SQLException;

/*
https://github.com/h2database/h2database/blob/45b609dec0e45125e6a93f85c9018d34551332a1/h2/src/main/org/h2/jdbc/JdbcBlob.java
 */
public class JdbcBlob extends BigFieldBase<byte[],JdbcBlob,Blob,InputStream> implements Blob  {


    public JdbcBlob(){

    }

    @Override
    protected byte[] sqlObjectToDataArray(Blob input, long length) throws SQLException, IOException {
        return sourceToDataArray(input.getBinaryStream(),length);
    }

    @Override
    protected byte[] sourceToDataArray(InputStream input, long length) throws IOException {
        if(length==-1) {
            return IOUtils.toByteArray(input);
        }else{
            return IOUtils.toByteArray(input,length);
        }
    }

    @Override
    protected void freeSqlObject(Blob input) throws SQLException {
        input.free();
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return getBinaryStream(0, Array.getLength(data));
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return new ByteArrayInputStream(this.data, (int)pos, (int)length);
    }



    private static final long   MIN_POS  = 1L;
    private static final long   MAX_POS  = MIN_POS + (long) Integer.MAX_VALUE;

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes == null ? 0
                : bytes.length);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        final int index = (int) (pos - MIN_POS);
        if(data==null){
            data = new byte[0];
        }
        final int dlen  = data.length;

        if (index > dlen - len) {
            byte[] temp = new byte[index + len];

            System.arraycopy(data, 0, temp, 0, dlen);

            data = temp;
            temp = null;
        }

        System.arraycopy(bytes, offset, data, index, len);
        this.length = data.length;

        return len;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        final byte[] result = new byte[length];
        System.arraycopy(this.data, (int)pos - 1, result, 0, length);
        return result;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return new ByteArrayOutputStream() {

            private boolean closed;

            public synchronized void close() throws IOException {

                if (closed) {
                    return;
                }

                closed = true;

                byte[] bytes  = super.buf;
                int    length = super.count;

                super.buf   = new byte[0];
                super.count = 0;

                try {
                    setBytes(pos, bytes, 0, length);
                } catch (SQLException se) {
                    throw new IOException(se);
                } finally {
                    super.close();
                }
            }
        };
    }

    @Override
    public void truncate(long len) throws SQLException {

        if (len == data.length) {
            return;
        }

        byte[] newData = new byte[(int) len];

        System.arraycopy(data, 0, newData, 0, (int) len);
        data = newData;
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        throw new UnsupportedOperationException("?position");
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
