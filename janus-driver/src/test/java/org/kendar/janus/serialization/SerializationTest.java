package org.kendar.janus.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.kendar.janus.cmd.connection.ConnectionConnect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SerializationTest {
    ObjectMapper mapper = new ObjectMapper();

    @Test
    void testConnectCommand() {
        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var properties = new Properties();
        properties.put("test", "value");
        var clientInfo = new Properties();
        var data = new ConnectionConnect(("https://www.google.com"), properties, clientInfo);
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (ConnectionConnect) serializer.read("command");
        assertEquals(data, deserialized);
    }

    @Test
    void testProperties() throws JsonProcessingException {
        var prop = new Properties();
        prop.put("test", "value");
        var json = mapper.writeValueAsString(prop);
        var value = mapper.readValue(json, Properties.class);
        assertEquals(prop, value);
    }


    @Test
    void testArrayListOfString() {
        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var data = new ArrayList<String>();
        data.add("test");
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (ArrayList<String>) serializer.read("command");
        assertEquals(data, deserialized);
    }

    @Test
    void testArrayOfString() {
        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var data = new String[]{"test"};
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (String[]) serializer.read("command");
        assertArrayEquals(data, deserialized);
    }



    @Test
    void testArrayOfInt() {
        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var data = new int[]{22};
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (int[]) serializer.read("command");
        assertArrayEquals(data, deserialized);
    }


    @Test
    void testArrayOfObjects() {
        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var properties = new Properties();
        properties.put("test", "value");
        var clientInfo = new Properties();
        var command = new ConnectionConnect(("https://www.google.com"), properties, clientInfo);
        var data = new Object[]{command};
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (Object[]) serializer.read("command");
        assertArrayEquals(data, deserialized);
    }


    @Test
    void testMapOfString() {
        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var data = new HashMap<String, String>();
        data.put("key", "value");
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (HashMap<String, String>) serializer.read("command");
        assertEquals(data, deserialized);
    }


    @Test
    void testMapOfCommands() {

        var properties = new Properties();
        properties.put("test", "value");
        var clientInfo = new Properties();
        var command = new ConnectionConnect(("https://www.google.com"), properties, clientInfo);

        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var data = new HashMap<String, TypedSerializable>();
        data.put("key", command);
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (HashMap<String, TypedSerializable>) serializer.read("command");
        assertEquals(data, deserialized);
    }


    @Test
    void testArrayOfCommands() {
        var properties = new Properties();
        properties.put("test", "value");
        var clientInfo = new Properties();
        var command = new ConnectionConnect(("https://www.google.com"), properties, clientInfo);

        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var data = new ArrayList<TypedSerializable>();
        data.add(command);
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (ArrayList<TypedSerializable>) serializer.read("command");
        assertEquals(data, deserialized);
    }


    @Test
    void testArrayOfMaps() {
        var properties = new Properties();
        properties.put("test", "value");

        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var data = new ArrayList<Properties>();
        data.add(properties);
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (ArrayList<Properties>) serializer.read("command");
        assertEquals(data, deserialized);
    }


    @Test
    void testArrayOfArrays() {
        var properties = new ArrayList<String>();
        properties.add("test");
        properties.add("value");

        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var data = new ArrayList<ArrayList<String>>();
        data.add(properties);
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (ArrayList<ArrayList<String>>) serializer.read("command");
        assertEquals(data, deserialized);
    }



    @Test
    void testMultidimensionalArrayOfString() {
        var serializer = (TypedSerializer) new JsonTypedSerializer();
        int[][] data = { { 1, 2 }, { 3, 4,5 } };

        for (int i = 0; i < data.length; i++) {
            var subData = data[i];
            for (int j = 0; j < subData.length; j++) {
                System.out.println("arr[" + i + "][" + j + "] = "
                        + data[i][j]);
            }
        }
        serializer.write("command", data);
        var result = serializer.getSerialized();
        serializer.deserialize(result);
        var deserialized = (int[][]) serializer.read("command");
        assertArrayEquals(data, deserialized);
    }

    @Test
    void testConnectCommandSimple() {
        String expected ="{\n" +
                "  \"command\" : {\n" +
                "    \"database\" : \"https://www.google.com\",\n" +
                "    \"properties\" : [ {\n" +
                "      \"_key\" : \"test\",\n" +
                "      \"_value\" : \"value\"\n" +
                "    } ],\n" +
                "    \"clientinfo\" : [ ]\n" +
                "  }\n" +
                "}";
        var serializer = (TypedSerializer) new JsonTypedSerializer();
        var properties = new Properties();
        properties.put("test", "value");
        var clientInfo = new Properties();
        var data = new ConnectionConnect(("https://www.google.com"), properties, clientInfo);
        serializer.write("command", data);
        var result = (String)serializer.getSimpleSerialized();
        assertEquals(expected,result);
    }


}
