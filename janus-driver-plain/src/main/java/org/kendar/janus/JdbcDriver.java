package org.kendar.janus;

import org.kendar.janus.cmd.connection.ConnectionConnect;
import org.kendar.janus.engine.DriverEngine;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.utils.ExceptionsWrapper;
import org.kendar.janus.utils.LocalProperties;
import org.kendar.janus.utils.LoggerWrapper;
import org.kendar.janus.utils.TimedOutConnection;

import java.net.URI;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class JdbcDriver implements Driver {

    static ConcurrentHashMap<Long, TimedOutConnection> connections = new ConcurrentHashMap<>();

    private static final Timer timer;
    private static final String JDBC_IDENTIFIER = "jdbc:janus:";
    private static final String HTTP_PROTOCOL = "http";
    private static final String HTTPS_PROTOCOL = "https";

    private static final LoggerWrapper log = LoggerWrapper.getLogger(JdbcDriver.class);
    private Engine engine;

    static {
        load();
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                expireConnections();
            }
        }, 0, 500);
    }


    private static synchronized void load() {
        try {
            DriverManager.registerDriver(new JdbcDriver());
            log.info("IjDriver JDBC-Driver successfully registered");
        }
        catch (SQLException e) {
            log.error("Unable to register IjDriver", e);
        }
    }
    private static Engine testEngine;

    public static void setTestEngine(Engine testEngineP){
        testEngine = testEngineP;
    }

    public JdbcDriver() {
        if (testEngine != null) {
            engine = testEngine;
        }
    }

    public static JdbcDriver of(Engine engine){
        var result = new JdbcDriver();
        result.engine = engine;
        return result;
    }

    @Override
    public Connection connect(String url, Properties properties) throws SQLException {


        Connection result = null;
        if (this.acceptsURL(url)) {
            try{
                var uri = URI.create(url.substring(JDBC_IDENTIFIER.length()));

                log.info("IjDriver-URL: {0}", uri);
                if(!isHttpOrHttps(uri)){
                    throw new SQLException("Unknown protocol: " + uri.getScheme());
                }
                String localProperties ="";
                if(properties!=null){
                    localProperties = properties.getProperty("janus.clientinfo.properties");
                }
                var engineToUse = engine;
                if(engineToUse==null){

                    log.info("Connecting");
                    engineToUse = new DriverEngine(uri.toString(),this);
                }
                var command = new ConnectionConnect(
                        url,
                        properties,
                        LocalProperties.build(localProperties));
                var connResult = (ObjectResult) engineToUse.execute(command,-1L,-1L);
                result = new JdbcConnection(connResult.getResult(), engineToUse);
                var toConnection = new TimedOutConnection();
                toConnection.setConnection(result);
                long time = Calendar.getInstance().getTimeInMillis();
                toConnection.setExpiration(time+5000);
                connections.put(connResult.getResult(),toConnection);
                log.info("Connected");
            }
            catch (Exception e)
            {
                log.error("Unable to connect", e);
                throw ExceptionsWrapper.toSQLException(e);
            }
        }
        return result;

    }


    protected static void expireConnections(){
        var all = new ArrayList<>(connections.entrySet());
        for(var context: all) {
            long time = Calendar.getInstance().getTimeInMillis();
            //noinspection CatchMayIgnoreException
            try {
                if (context.getValue().getConnection().isClosed() ||
                        time>context.getValue().getExpiration()) {
                    context.getValue().getConnection().close();
                    connections.remove(context.getKey());
                }
            }catch(Exception ex){

            }
        }
    }

    private boolean isHttpOrHttps(URI uri) {
        return (uri.getScheme().equalsIgnoreCase(HTTP_PROTOCOL) || uri.getScheme().equalsIgnoreCase(HTTP_PROTOCOL));
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(JDBC_IDENTIFIER);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger");
    }

    public void refreshConnection(Long connectionId) {
        if(!connections.containsKey(connectionId))return;
        long time = Calendar.getInstance().getTimeInMillis();
        connections.get(connectionId).setExpiration(time+5000);
    }
}
