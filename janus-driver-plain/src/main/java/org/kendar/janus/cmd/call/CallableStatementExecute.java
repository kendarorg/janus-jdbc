package org.kendar.janus.cmd.call;

import org.apache.commons.lang3.ClassUtils;
import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.cmd.preparedstatement.PreparedStatementExecuteBase;
import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
import org.kendar.janus.cmd.preparedstatement.parameters.ObjectParameter;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class CallableStatementExecute extends PreparedStatementExecuteBase {
    public CallableStatementExecute(){

    }

    private List<PreparedStatementParameter> outParameters;
    private boolean loadRsOnExec;

    public CallableStatementExecute(String sql, List<PreparedStatementParameter> parameters, List<PreparedStatementParameter> outParameters,boolean loadRsOnExec){
        super(sql,parameters);
        this.outParameters = outParameters;
        this.loadRsOnExec = loadRsOnExec;
    }


    @Override
    protected Object executeInternal(PreparedStatement statement) throws SQLException {
        var result = statement.execute();

        if(!loadRsOnExec){
            return result;
        }
        if(result){
            return statement.getResultSet();
        }
        return false;
    }


    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("sql",sql);
        builder.write("parameters",parameters);
        builder.write("outParameters",outParameters);
        builder.write("loadRsOnExec",loadRsOnExec);

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer input) {
        sql = input.read("sql");
        parameters = input.read("parameters");
        outParameters = input.read("outParameters");
        loadRsOnExec =input.read("loadRsOnExec");
        return this;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {

        if(ClassUtils.isAssignable(context.get(uid).getClass(), CallableStatement.class)){
            var statement = (CallableStatement) context.get(uid);
            for (int i = 0; i < parameters.size(); i++) {
                PreparedStatementParameter param = parameters.get(i);
                if (param == null) continue;
                param.load(statement);

            }
            if(outParameters!=null) {
                for (int i = 0; i < outParameters.size(); i++) {
                    ObjectParameter param = (ObjectParameter) outParameters.get(i);
                    if (param == null) continue;
                    param.loadOut(statement);

                }
            }
            return executeInternal(statement);
        }else {
            var statement = (PreparedStatement)context.get(uid);
            for (int i = 0; i < parameters.size(); i++) {
                PreparedStatementParameter param = parameters.get(i);
                if (param == null) continue;
                param.load(statement);

            }
            return executeInternal(statement);
        }
    }


    @Override
    public String getPath() {
        return "/CallableStatement/execute";
    }

}
