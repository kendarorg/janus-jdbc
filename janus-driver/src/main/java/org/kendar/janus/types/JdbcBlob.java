package org.kendar.janus.types;

import org.apache.commons.io.IOUtils;
import org.kendar.janus.utils.EmptyInputStream;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;

/*
https://github.com/h2database/h2database/blob/45b609dec0e45125e6a93f85c9018d34551332a1/h2/src/main/org/h2/jdbc/JdbcBlob.java
 */
public class JdbcBlob extends BigFieldBase<byte[],JdbcBlob,Blob,InputStream> implements Blob,JdbcType  {


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
        if(data ==null){
            return new EmptyInputStream();
        }
        return getBinaryStream(0, Array.getLength(data));
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        if(data ==null){
            return new EmptyInputStream();
        }
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
        throw new UnsupportedOperationException("?position");
    }

    @Override
    public Object toNativeObject(Connection connection) throws SQLException {
        var result = connection.createBlob();
        try {
            if(data==null)return result;
            result.setBinaryStream(1).write(data);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return result;
    }
    public String toString() {
        if(this.data==null)return "<closed>";

        return encode(this.data);
    }

    private static final char[] LOOKUP_TABLE_LOWER = new char[]{0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66};
    private static final char[] LOOKUP_TABLE_UPPER = new char[]{0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46};

    private static String encode(byte[] byteArray, boolean upperCase, ByteOrder byteOrder) {

        // our output size will be exactly 2x byte-array length
        final char[] buffer = new char[byteArray.length * 2];

        // choose lower or uppercase lookup table
        final char[] lookup = upperCase ? LOOKUP_TABLE_UPPER : LOOKUP_TABLE_LOWER;

        int index;
        for (int i = 0; i < byteArray.length; i++) {
            // for little endian we count from last to first
            index = (byteOrder == ByteOrder.BIG_ENDIAN) ? i : byteArray.length - i - 1;

            // extract the upper 4 bit and look up char (0-A)
            buffer[i << 1] = lookup[(byteArray[index] >> 4) & 0xF];
            // extract the lower 4 bit and look up char (0-A)
            buffer[(i << 1) + 1] = lookup[(byteArray[index] & 0xF)];
        }
        var partial= new String(buffer);
        var sb = new StringBuilder();
        for(var i=0;i<buffer.length;i+=2){
            sb.append("X'");
            sb.append(buffer[i]);
            sb.append(buffer[i+1]);
            sb.append("'");
        }
        return sb.toString();
    }

    private static String encode(byte[] byteArray) {
        return encode(byteArray, false, ByteOrder.BIG_ENDIAN);
    }
}
