package org.kendar.janus.server;

import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.serialization.JsonTypedSerializer;

import java.sql.SQLException;

public class JsonServer implements Engine {
    private final JsonTypedSerializer serializer;
    private Engine engine;

    public JsonServer(Engine engine){
        this.engine = engine;
        this.serializer = new JsonTypedSerializer();
    }

    @Override
    public JdbcResult execute(JdbcCommand command, Long connectionId, Long uid) throws SQLException {
        JdbcCommand deserialized = getIjCommand(command);
        var result = engine.execute(deserialized,connectionId,uid);
        return getIjResult(result);
    }

    @Override
    public int getMaxRows() {
        throw new RuntimeException();
    }

    @Override
    public boolean getPrefetchMetadata() {
        throw new RuntimeException();
    }

    @Override
    public String getCharset() {
        throw new RuntimeException();
    }

    private JdbcResult getIjResult(JdbcResult command) {
        var ser = serializer.newInstance();
        ser.write("command", command);
        var serialized = ser.getSerialized();
        var deser = serializer.newInstance();
        deser.deserialize(serialized);
        var deserialized = (JdbcResult)deser.read("command");
        return deserialized;
    }

    private JdbcCommand getIjCommand(JdbcCommand command) {
        var ser = serializer.newInstance();
        ser.write("command", command);
        var serialized = ser.getSerialized();
        var deser = serializer.newInstance();
        deser.deserialize(serialized);
        var deserialized = (JdbcCommand)deser.read("command");
        return deserialized;
    }
}
