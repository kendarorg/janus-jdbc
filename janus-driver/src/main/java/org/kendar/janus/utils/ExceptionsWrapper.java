package org.kendar.janus.utils;

import java.sql.SQLException;

public class ExceptionsWrapper
{
    public static SQLException toSQLException(Throwable e) {
        if (e instanceof SQLException) {
            return (SQLException)e;
        }
        return new SQLException(e);
    }


}
