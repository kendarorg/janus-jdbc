package org.kendar.janus.utils;

import java.sql.SQLException;

@FunctionalInterface
public interface SqlExceptionFunction<T, R> {

        R apply(T t) throws SQLException;
}
