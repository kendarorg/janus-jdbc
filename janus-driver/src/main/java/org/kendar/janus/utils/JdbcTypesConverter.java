package org.kendar.janus.utils;

import org.apache.commons.lang3.ClassUtils;
import org.kendar.janus.JdbcDatabaseMetaData;
import org.kendar.janus.JdbcResultSet;
import org.kendar.janus.JdbcResultsetMetaData;
import org.kendar.janus.JdbcSavepoint;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.enums.ResultSetConcurrency;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.results.StatementResult;
import org.kendar.janus.results.VoidResult;
import org.kendar.janus.types.*;

import java.sql.*;
import java.util.function.Function;
import java.util.function.Supplier;

public class JdbcTypesConverter {

    public static JdbcResult convertResult(Engine engine, Object resultObject, Long connectionId, Supplier<Long> traceId) throws SQLException {
        if(resultObject==Void.TYPE){
            return new VoidResult();
        }
        if(resultObject==null){
            return new ObjectResult(null);
        }
        if(resultObject.getClass()==String.class){
            return new ObjectResult(resultObject);
        }
        if(ClassUtils.isPrimitiveOrWrapper(resultObject.getClass())){
            return new ObjectResult(resultObject);
        }
        if(ClassUtils.isAssignable(resultObject.getClass(), Savepoint.class)){
            var result = new JdbcSavepoint();
            result.setTraceId(traceId.get());
            var ori = (Savepoint)resultObject;
            try {
                result.setSavePointId(ori.getSavepointId());
            }catch (Exception ex){}
            try {
                result.setSavePointName(ori.getSavepointName());
            }catch (Exception ex){}
            return result;
        }
        if(ClassUtils.isAssignable(resultObject.getClass(), Statement.class)){
            var stmt = (Statement)resultObject;
            var maxRows = stmt.getMaxRows();
            if(maxRows==0){
                maxRows = engine.getMaxRows();
            }
            return new StatementResult(traceId.get(),maxRows,stmt.getQueryTimeout());
        }
        if(ClassUtils.isAssignable(resultObject.getClass(), ResultSetMetaData.class)){
            var rstst = (ResultSetMetaData)resultObject;
            var result = new JdbcResultsetMetaData();
            result.initialize(rstst);
            return result;
        }if(ClassUtils.isAssignable(resultObject.getClass(), DatabaseMetaData.class)){
            var rstst = (DatabaseMetaData)resultObject;
            var result = new JdbcDatabaseMetaData();
            result.fill(rstst);
            result.setTraceId(traceId.get());
            return result;
        }
        if(ClassUtils.isAssignable(resultObject.getClass(), ResultSet.class)){
            var rstst = (ResultSet)resultObject;
            var stmt = (Statement)rstst.getStatement();
            var maxRows = 0;

            if(stmt!=null) {
                maxRows = stmt.getMaxRows();
            }
            if(maxRows==0){
                maxRows = engine.getMaxRows();
            }
            var result = new JdbcResultSet(
                    traceId.get(),
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
        if(resultObject.getClass().isArray()){
            return new ObjectResult(resultObject);
        }
        return new ObjectResult(null);
    }

    public static Object convertToSerializable(Object object) throws SQLException {
        if(object==null) return null;
        var obClass= object.getClass();
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
