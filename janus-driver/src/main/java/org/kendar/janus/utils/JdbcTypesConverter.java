package org.kendar.janus.utils;

import org.apache.commons.lang3.ClassUtils;
import org.kendar.janus.JdbcResultSet;
import org.kendar.janus.JdbcResultsetMetaData;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.enums.ResultSetConcurrency;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.results.StatementResult;
import org.kendar.janus.types.*;

import java.sql.*;

public class JdbcTypesConverter {

    public static JdbcResult convertResult(Engine engine, Object resultObject, Long connectionId, Long traceId) throws SQLException {
        if(resultObject==null){
            return new ObjectResult(null);
        }
        if(ClassUtils.isPrimitiveOrWrapper(resultObject.getClass())){
            return new ObjectResult(resultObject);
        }
        if(ClassUtils.isAssignable(resultObject.getClass(), Statement.class)){
            var stmt = (Statement)resultObject;
            var maxRows = stmt.getMaxRows();
            if(maxRows==0){
                maxRows = engine.getMaxRows();
            }
            return new StatementResult(traceId,maxRows,stmt.getQueryTimeout());
        }
        if(ClassUtils.isAssignable(resultObject.getClass(), ResultSetMetaData.class)){
            var rstst = (ResultSetMetaData)resultObject;
            var result = new JdbcResultsetMetaData();
            result.initialize(rstst);
            return result;
        }
        if(ClassUtils.isAssignable(resultObject.getClass(), ResultSet.class)){
            var rstst = (ResultSet)resultObject;
            var stmt = (Statement)rstst.getStatement();
            var maxRows = stmt.getMaxRows();
            if(maxRows==0){
                maxRows = engine.getMaxRows();
            }
            var result = new JdbcResultSet(
                    traceId,
                    ResultSetType.valueOf((int)rstst.getType()),
                    maxRows,
                    engine.getPrefetchMetadata(),
                    engine.getCharset(),
                    ResultSetConcurrency.valueOf((int)rstst.getConcurrency()),
                    ResultSetHoldability.valueOf(rstst.getHoldability()));
            result.intialize(rstst);
            return result;
        }
        if(ClassUtils.isAssignable(resultObject.getClass(), JdbcResult.class)){
            return (JdbcResult) resultObject;
        }
        return new ObjectResult(null);
    }

    public static Object convertToSerializable(Object object) throws SQLException {
        if(object==null) return null;
        if(ClassUtils.isAssignable(object.getClass(),Struct.class)){
            return new JdbcStruct().fromStruct((Struct)object);
        }else if(ClassUtils.isAssignable(object.getClass(),Array.class)){
            return new JdbcArray().fromSqlArray((Array)object);
        }else if(ClassUtils.isAssignable(object.getClass(),NClob.class)){
            return new JdbcNClob().fromSqlType((NClob) object);
        }else if(ClassUtils.isAssignable(object.getClass(),Clob.class)){
            return new JdbcClob().fromSqlType((Clob) object);
        }else if(ClassUtils.isAssignable(object.getClass(),Blob.class)){
            return new JdbcBlob().fromSqlType((Blob) object);
        }else if(ClassUtils.isAssignable(object.getClass(),SQLXML.class)){
            return new JdbcSQLXML().fromSqlType((SQLXML) object);
        }else if(ClassUtils.isAssignable(object.getClass(),RowId.class)){
            return new JdbcRowId().fromObject(object);
        }
        return object;
    }
}