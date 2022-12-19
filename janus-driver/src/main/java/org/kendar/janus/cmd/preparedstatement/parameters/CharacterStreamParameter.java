package org.kendar.janus.cmd.preparedstatement.parameters;

import org.apache.commons.io.IOUtils;

import java.io.CharArrayReader;
import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CharacterStreamParameter extends SimpleParameter<char[]>{
    public CharacterStreamParameter fromReader(Reader reader, int length) throws SQLException {
        try{
            if(reader==null){
                value=null;
                return this;
            }
            var tmp= IOUtils.toCharArray(reader);
            var maxLen = (int)Math.min(length,tmp.length);
            if(maxLen<0){
                maxLen = (int)Math.max(length,tmp.length);
            }
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

    @Override
    public void load(CallableStatement callableStatement) throws SQLException {
        if(hasColumnName())callableStatement.setCharacterStream(columnName,new CharArrayReader(this.value));
        else load((PreparedStatement) callableStatement);
    }
}
