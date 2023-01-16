package org.kendar.janus.utils;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.kendar.janus.JdbcDriver;
import org.kendar.janus.server.JsonServer;
import org.kendar.janus.server.ServerEngine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.UUID;

@SuppressWarnings("resource")
public class TestBase {
    
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getRealConnection(), "sa", "sa");
    }

    public String getRealConnection(){
        return "jdbc:h2:mem:test;";
    }

    protected void beforeEach() throws SQLException {

        serverEngine = new ServerEngine(getRealConnection(), "sa", "sa");
        jsonServer = new JsonServer(serverEngine);
        driver = JdbcDriver.of(jsonServer);
        //if(!initialized){
        //synchronized (locker) {
        var conn = getConnection();
        conn.createStatement().execute("DROP ALL OBJECTS");
        conn.close();
        initialized=true;
        //}
        //}
    }

    protected final String CONNECT_URL = "jdbc:janus:http://localhost/db?fetchSize=3&charset=UTF-8";
    protected Driver driver;
    private static final String dbId= UUID.randomUUID().toString();
    protected ServerEngine serverEngine;
    protected JsonServer jsonServer;
    protected SessionFactory sessionFactory;

    protected Connection createFooTable() throws SQLException {
        var conn = getConnection();
        String createStatement = "create table if not exists bar  (foo varchar(50))";

        Statement stmt = conn.createStatement();
        stmt.executeUpdate(createStatement);
        return conn;
    }

    protected void createFooTableWithField(String value) throws SQLException {
        var conn = createFooTable();
        conn.createStatement().execute("INSERT INTO bar (foo) VALUES('" + value + "')");
    }
    private boolean initialized=false;
    private final Object locker = new Object();

    protected InputStreamReader getInputStreamReader(int kb) throws IOException {
        var bytes = getBytes(kb);
        var inputStream = new ByteArrayInputStream(bytes);
        return new InputStreamReader(inputStream);
    }

    protected Object invokeGet(Object ps, String method,Object index) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        var realTypes = new Class<?>[1];
        if(index instanceof String){
            realTypes[0]=String.class;
        }else{
            realTypes[0]=int.class;
        }
        var mt = ps.getClass().getMethod("get"+method,realTypes);
        return mt.invoke(ps, index);
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
        var conn = getConnection();
        //DatabaseMetaData databaseMetaData = conn.getMetaData();

        conn.createStatement().execute("DROP ALL OBJECTS");
        conn.close();
    }

    protected void createSimpleTable() throws SQLException {
        var conn = getConnection();
        conn.createStatement().execute("create table if not exists " +
                "persons(" +
                "id IDENTITY NOT NULL PRIMARY KEY, " +
                "name varchar(255));");
    }



    protected void insertInSimpleTable(String value) throws SQLException {
        var conn = getConnection();
        conn.createStatement().execute("INSERT INTO persons(NAME) VALUES ('"+value+"');");
    }

    protected void updateInSimpleTable(String oldValue,String newValue) throws SQLException {
        var conn = getConnection();
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

    protected void setupHibernate(Class<?> ... tables){
        JdbcDriver.setTestEngine(jsonServer);
        var hibernateConfig = new Configuration();

        hibernateConfig.setProperty("hibernate.connection.driver_class", "org.kendar.janus.JdbcDriver");
        hibernateConfig.setProperty("hibernate.connection.url", CONNECT_URL);
        hibernateConfig.setProperty("hibernate.connection.username", "sa");
        hibernateConfig.setProperty("hibernate.connection.password", "sa");
        hibernateConfig.setProperty("hibernate.dialect", "org.hibernate.dialect.H2Dialect");
        hibernateConfig.setProperty("show_sql", "true");
        hibernateConfig.setProperty("hibernate.hbm2ddl.auto", "update");
        for(var table:tables){
            hibernateConfig.addAnnotatedClass(table);
        }
        sessionFactory = hibernateConfig.buildSessionFactory();
    }
}
