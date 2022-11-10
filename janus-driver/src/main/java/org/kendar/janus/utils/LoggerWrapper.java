package org.kendar.janus.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

public class LoggerWrapper {
    private Logger logger;

    public LoggerWrapper(Logger logger) {

        this.logger = logger;
    }


    public static LoggerWrapper getLogger(Class<?> clazz){
        return new LoggerWrapper(Logger.getLogger(clazz.getName()));
    }

    public void info(String s) {

    }

    public void error(String msg, Throwable e) {
        logger.log(Level.SEVERE,msg,e);
    }

    public void info(String message, Object ... data) {
        logger.log(Level.INFO,message,data);
    }

    public void debug(String message) {
        logger.log(Level.FINE,message);
    }
}
