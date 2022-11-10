package org.kendar.janus.utils;

import org.kendar.janus.JdbcDriver;
import org.kendar.janus.server.JsonServer;
import org.kendar.janus.server.ServerEngine;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;

public class TestBase {
    protected final String CONNECT_URL = "jdbc:janus:http://localhost/db?fetchSize=3&charset=UTF-8";
    protected Driver driver;
    private ServerEngine serverEngine;
    private JsonServer jsonServer;

    protected void beforeEach() {
        serverEngine = new ServerEngine("jdbc:h2:mem:test;", "sa", "sa");
        jsonServer = new JsonServer(serverEngine);
        driver = (Driver)new JdbcDriver(jsonServer);
    }


    protected Object invokeGet(Object ps, String method,Object index) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        var realTypes = new Class<?>[1];
        if(index instanceof String){
            realTypes[0]=String.class;
        }else{
            realTypes[0]=int.class;
        }
        var mt = ps.getClass().getMethod("get"+method,realTypes);
        return mt.invoke(ps,new Object[]{index});
    }

    protected void invokeSet(Object ps,Class<?>[] input, Object[] value, String method,Object index) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        var realTypes = new Class<?>[value.length+1];
        var realVals = new Object[value.length+1];

        realVals[0] = index;
        if(index instanceof String){
            realTypes[0]=String.class;
        }else{
            realTypes[0]=int.class;
        }
        for (int i = 0; i < value.length; i++) {
            realVals[i+1]=value[i];
            realTypes[i+1]=input[i];
        }
        var mt = ps.getClass().getMethod("set"+method,realTypes);
        mt.invoke(ps,realVals);
    }



    protected void invokeUpdate(Object ps,Class<?>[] input, Object[] value, String method,Object index) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        var realTypes = new Class<?>[value.length+1];
        var realVals = new Object[value.length+1];

        realVals[0] = index;
        if(index instanceof String){
            realTypes[0]=String.class;
        }else{
            realTypes[0]=int.class;
        }
        for (int i = 0; i < value.length; i++) {
            realVals[i+1]=value[i];
            realTypes[i+1]=input[i];
        }
        var mt = ps.getClass().getMethod("update"+method,realTypes);
        mt.invoke(ps,realVals);
    }

    protected void afterEach() throws SQLException {
        var conn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        var tablesCount =0;
        try(ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"})){
            while(resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String schema = resultSet.getString("TABLE_SCHEM");
                if(schema.equalsIgnoreCase("public")) {
                    conn.createStatement().execute("DROP TABLE "+tableName);
                }
            }
        }
        conn.close();
    }

    protected void createSimpleTable() throws SQLException {
        var conn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        conn.createStatement().execute("create table if not exists " +
                "persons(" +
                "id IDENTITY NOT NULL PRIMARY KEY, " +
                "name varchar(255));");
    }



    protected void insertInSimpleTable(String value) throws SQLException {
        var conn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        conn.createStatement().execute("INSERT INTO persons(NAME) VALUES ('"+value+"');");
    }

    protected void updateInSimpleTable(String oldValue,String newValue) throws SQLException {
        var conn = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        conn.createStatement().execute("UPDATE persons SET NAME='"+newValue+"' ;");
    }

    protected char[] getCharacters(int kb) throws IOException {
        char[] buffer = new char[1_000*kb];
        for (int j = 0; j < 1_000*kb; j++) {
            buffer[j] = ((char)(35 + (j % 90)));
        }
        return buffer;
    }


    protected byte[] getBytes(int kb) throws IOException {
        var chars = getCharacters(kb);
        var result = new byte[1_000*kb];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) chars[i];
        }
        return result;
    }
}
