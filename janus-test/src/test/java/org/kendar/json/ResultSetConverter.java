package org.kendar.json;

import org.kendar.janus.JdbcResultSet;
import org.kendar.janus.serialization.JsonTypedSerializer;

public class ResultSetConverter {
    private static JsonTypedSerializer serializer = new JsonTypedSerializer();


    public HamResultSet toHam(JdbcResultSet resultSet){
        var ser = serializer.newInstance();
        ser.write("rs", resultSet);
        String serialized = (String)ser.getSerialized();
        serialized= serialized.replace("org.kendar.janus.JdbcResultSet","org.kendar.json.HamResultSet");

        var deser = serializer.newInstance();
        deser.deserialize(serialized);
        return deser.read("rs");
    }

    public JdbcResultSet fromHam(HamResultSet resultSet){
        var ser = serializer.newInstance();
        ser.write("rs", resultSet);
        String serialized = (String)ser.getSerialized();
        serialized = serialized.replace("org.kendar.json.HamResultSet","org.kendar.janus.JdbcResultSet");

        var deser = serializer.newInstance();
        deser.deserialize(serialized);
        return deser.read("rs");
    }
}
