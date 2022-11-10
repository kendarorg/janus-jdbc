package org.kendar.janus.cmd.preparedstatement.parameters;

import org.apache.commons.io.IOUtils;

import java.io.CharArrayReader;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CharacterStreamParameter extends SimpleParameter<char[]>{
    public CharacterStreamParameter fromReader(Reader reader, int length) throws SQLException {
        try{
            var tmp= IOUtils.toCharArray(reader);
            var maxLen = (int)Math.min(length,tmp.length);
            this.value = new char[maxLen];
            System.arraycopy(tmp, 0, value, 0, maxLen);

        }catch (Exception ex){
            throw new SQLException(ex);
        }
        return this;
    }

    public CharacterStreamParameter fromReader(Reader reader) throws SQLException {
        try{
            this.value =IOUtils.toCharArray(reader);
        }catch (Exception ex){
            throw new SQLException(ex);
        }
        return this;
    }

    @Override
    public void load(PreparedStatement preparedStatement) throws SQLException {

        preparedStatement.setCharacterStream(columnIndex, new CharArrayReader(this.value));
    }
}
