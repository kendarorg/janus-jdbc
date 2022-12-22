package org.kendar.janus.server;

import org.kendar.janus.cmd.Exec;
import org.kendar.janus.cmd.JdbcCommand;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.serialization.JsonTypedSerializer;

import java.sql.SQLException;
import java.util.UUID;

public class JsonServer implements Engine {
    private final JsonTypedSerializer serializer;
    private Engine engine;

    public JsonServer(Engine engine){
        this.engine = engine;
        this.serializer = new JsonTypedSerializer();
    }

    @Override
    public Engine create() {
        return new JsonServer(engine);
    }

    @Override
    public JdbcResult execute(JdbcCommand command, Long connectionId, Long uid) throws SQLException {
        JdbcCommand deserialized = getIjCommand(command);

        System.out.println("============================================");
        var toPrint = deserialized.toString();
        if(toPrint.length()>1000){
            toPrint = toPrint.substring(0,1000);
        }
        System.out.println("INPUT "+toPrint);
        var result = engine.execute(deserialized,connectionId,uid);

        if(result instanceof ObjectResult){
            var res =((ObjectResult)result).getResult();
            if(res!=null){
                System.out.println("--------------------------------------------");
                System.out.println("\tOUTPUT "+res.getClass().getSimpleName());
            }
        }else{
            if(result!=null){
                System.out.println("--------------------------------------------");
                System.out.println("\tOUTPUT "+result.getClass().getSimpleName());
            }
        }
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

    @Override
    public UUID startRecording() {
        return engine.startRecording();
    }

    @Override
    public void cleanRecordings() {
        engine.cleanRecordings();
    }

    @Override
    public void stopRecording(UUID id) {
engine.stopRecording(id);
    }

    @Override
    public void startReplaying(UUID id) {
engine.startReplaying(id);
    }

    @Override
    public void stopReplaying(UUID id) {
engine.stopReplaying(id);
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
