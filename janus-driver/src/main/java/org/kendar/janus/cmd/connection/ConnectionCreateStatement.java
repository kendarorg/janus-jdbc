package org.kendar.janus.cmd.connection;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.enums.ResultSetConcurrency;
import org.kendar.janus.enums.ResultSetHoldability;
import org.kendar.janus.enums.ResultSetType;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionCreateStatement implements JdbcCommand {
    private ResultSetType type;

    @Override
    public String toString() {
        return "ConnectionCreateStatement{" +
                "\n\ttype=" + type +
                ", \n\tconcurrency=" + concurrency +
                ", \n\tholdability=" + holdability +
                '}';
    }

    public ResultSetType getType() {
        return type;
    }

    public ResultSetConcurrency getConcurrency() {
        return concurrency;
    }

    public ResultSetHoldability getHoldability() {
        return holdability;
    }

    private ResultSetConcurrency concurrency;
    private ResultSetHoldability holdability;

    public ConnectionCreateStatement(){
        type = ResultSetType.FORWARD_ONLY;
        concurrency = ResultSetConcurrency.CONCUR_READ_ONLY;
        holdability = ResultSetHoldability.DEFAULT;
    }

    public ConnectionCreateStatement withType(ResultSetType type){

        this.type = type;
        return this;
    }

    public ConnectionCreateStatement withConcurrency(ResultSetConcurrency concurrency){

        this.concurrency = concurrency;
        return this;
    }

    public ConnectionCreateStatement withHoldability(ResultSetHoldability holdability){

        this.holdability = holdability;
        return this;
    }

    @Override
    public Object execute(JdbcContext context, Long uid) throws SQLException {
        var connection = (Connection) context.getConnection();
        if (type == null && concurrency == null) {
            return connection.createStatement();
        }
        /*if(!connection.getMetaData().supportsResultSetType(type.getValue())){
            throw new SQLException(type+" not supported");
        }
        if(!connection.getMetaData().supportsResultSetConcurrency(type.getValue(),concurrency.getValue())){
            throw new SQLException(type+":"+concurrency+" not supported");
        }*/
        if(holdability!=ResultSetHoldability.DEFAULT){
            if(!connection.getMetaData().supportsResultSetHoldability(holdability.getValue())){
                throw new SQLException(holdability+" not supported");
            }
            return connection.createStatement(type.getValue(), concurrency.getValue(),holdability.getValue());
        }
        return connection.createStatement(type.getValue(), concurrency.getValue());
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("type",type);
        builder.write("concurrency",concurrency);
        builder.write("holdability",holdability);

    }

    @Override
    public JdbcCommand deserialize(TypedSerializer builder) {
        type = builder.read("type");
        concurrency =builder.read("concurrency");
        holdability = builder.read("holdability");
        return this;
    }
    @Override
    public String getPath() {
        return "/Connection/createStatement";
    }
}
