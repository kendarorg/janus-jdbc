package com.toddfast.util.convert.conversion;

public class SqlUtilDate {

    public static java.sql.Date toSqlDate(Object date){
        if(date instanceof java.util.Date) {
            return new java.sql.Date(((java.util.Date)date).getTime());
        }
        return (java.sql.Date)date;
    }
}
