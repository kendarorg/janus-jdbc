package org.kendar.janus.cmd.connection;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.serialization.TypedSerializer;
import org.kendar.janus.server.JdbcContext;

import java.util.Objects;
import java.util.Properties;

public class ConnectionConnect implements JdbcCommand {
    private String database;
    private Properties properties;
    private Properties clientInfo;

    public ConnectionConnect(){

    }


    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public Properties getClientInfo() {
        return clientInfo;
    }

    public void setClientInfo(Properties clientInfo) {
        this.clientInfo = clientInfo;
    }

    public ConnectionConnect(String uri, Properties properties, Properties clientInfo) {
        this.database = uri;
        this.properties = properties;
        this.clientInfo = clientInfo;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("database",database);
        builder.write("properties",properties);
        builder.write("clientInfo",clientInfo);
    }

    @Override
    public ConnectionConnect deserialize(TypedSerializer input) {
        database = input.read("database");
        properties = input.read("properties");
        clientInfo = input.read("clientInfo");
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, properties, clientInfo);
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }


    @Override
    public Object execute(JdbcContext context, Long uid) {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectionConnect that = (ConnectionConnect) o;
        return database.equals(that.database) && Objects.equals(properties, that.properties) && Objects.equals(clientInfo, that.clientInfo);
    }

    @Override
    public String toString() {
        return "ConnectionConnect{" +
                "\n\tdatabase='" + database + '\'' +
                ", \n\tproperties=" + properties +
                ", \n\tclientInfo=" + clientInfo +
                '}';
    }


    @Override
    public String getPath() {
        return "/Connection/connect";
    }
}
