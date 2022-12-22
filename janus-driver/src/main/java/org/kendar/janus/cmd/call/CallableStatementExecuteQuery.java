package org.kendar.janus.cmd.call;

import org.apache.commons.lang3.ClassUtils;
import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.cmd.preparedstatement.PreparedStatementExecuteBase;
import org.kendar.janus.cmd.preparedstatement.PreparedStatementParameter;
import org.kendar.janus.cmd.preparedstatement.parameters.ObjectParameter;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class CallableStatementExecuteQuery extends PreparedStatementExecuteBase {

    private List<PreparedStatementParameter> outParameters;

    public CallableStatementExecuteQuery(){

    }

    public CallableStatementExecuteQuery(String sql, List<PreparedStatementParameter> parameters, List<PreparedStatementParameter> outParameters) {
        super(sql,parameters);
        this.outParameters = outParameters;
    }

    @Override
    protected Object executeInternal(PreparedStatement statement) throws SQLException {
        return statement.executeQuery();
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
    public void serialize(TypedSerializer builder) {
        builder.write("sql",sql);
        builder.write("parameters",parameters);
        builder.write("outParameters",outParameters);

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer input) {
        sql = input.read("sql");
        parameters = input.read("parameters");
        outParameters = input.read("outParameters");
        return this;
    }


    @Override
    public String getPath() {
        return "/CallableStatement/executeQuery";
    }
}
