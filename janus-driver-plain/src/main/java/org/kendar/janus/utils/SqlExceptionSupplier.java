package org.kendar.janus.utils;

import java.sql.SQLException;

@FunctionalInterface
public interface SqlExceptionSupplier<T> {
    T get() throws SQLException;

}
