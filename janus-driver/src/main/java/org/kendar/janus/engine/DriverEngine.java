package org.kendar.janus.engine;

import org.kendar.janus.JdbcDriver;
import org.kendar.janus.cmd.interfaces.JdbcCommand;
import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.serialization.JsonTypedSerializer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;

public class DriverEngine implements Engine {
    private static final JsonTypedSerializer serializer = new JsonTypedSerializer();
    private String url;
    private JdbcDriver jdbcDriver;

    public DriverEngine(String url, JdbcDriver jdbcDriver){
        this.url = url;
        this.jdbcDriver = jdbcDriver;
    }

    @Override
    public Engine create() {
        return new DriverEngine(url, jdbcDriver);
    }

    public JdbcResult execute(JdbcCommand command, Long connectionId, Long uid){
        jdbcDriver.refreshConnection(connectionId);
        var ser = serializer.newInstance();
        try {
            var path = url+command.getPath()+"/"+uid;
            if(command.getPath().startsWith("/Connection/")){
                path = url+command.getPath();
            }
            ser.write("command", command);
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .header("Content-type","application/json")
                    .header("X-Connection-Id",connectionId.toString())
                    .uri(URI.create(path))
                    .POST(HttpRequest.BodyPublishers.ofString((String) ser.getSerialized()))
                    .build();

            HttpResponse<String> response = client.send(request,
                    HttpResponse.BodyHandlers.ofString());

            var deser = serializer.newInstance();
            deser.deserialize(response.body());
            return (JdbcResult)deser.read("result");
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

    @Override
    public int getMaxRows() {
        return 100;
    }

    @Override
    public boolean getPrefetchMetadata() {
        return false;
    }

    @Override
    public String getCharset() {
        return null;
    }

    @Override
    public UUID startRecording() {
        return null;
    }

    @Override
    public void cleanRecordings() {

    }

    @Override
    public void stopRecording(UUID id) {

    }

    @Override
    public void startReplaying(UUID id) {

    }

    @Override
    public void stopReplaying(UUID id) {

    }
}
