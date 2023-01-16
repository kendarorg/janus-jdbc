package org.kendar.janus.utils;

import java.util.Locale;

public class JdbcArrayTypeTranslator {

    public static int translateToBaseType(String typeName) {
        int baseType;
        typeName = typeName.toLowerCase(Locale.ROOT);
        switch (typeName) {
            case ("array"): {
                baseType = 2003;
                break;
            }
            case ("bigint"): {
                baseType = -5;
                break;
            }
            case ("binary"): {
                baseType = -2;
                break;
            }
            case ("bit"): {
                baseType = -7;
                break;
            }
            case ("blob"): {
                baseType = 2004;
                break;
            }
            case ("char"): {
                baseType = 1;
                break;
            }
            case ("clob"): {
                baseType = 2005;
                break;
            }
            case ("datalink"): {
                baseType = 70;
                break;
            }
            case ("date"): {
                baseType = 91;
                break;
            }
            case ("decimal"): {
                baseType = 3;
                break;
            }
            case ("distinct"): {
                baseType = 2001;
                break;
            }
            case ("double"): {
                baseType = 8;
                break;
            }
            case ("float"): {
                baseType = 6;
                break;
            }
            case ("integer"): {
                baseType = 4;
                break;
            }
            case ("java_object"): {
                baseType = 2000;
                break;
            }
            case ("longnvarchar"): {
                baseType = -16;
                break;
            }
            case ("longvarbinary"): {
                baseType = -4;
                break;
            }
            case ("longvarchar"): {
                baseType = -1;
                break;
            }
            case ("nchar"): {
                baseType = -15;
                break;
            }
            case ("nclob"): {
                baseType = 2011;
                break;
            }
            case ("null"): {
                baseType = 0;
                break;
            }
            case ("numeric"): {
                baseType = 2;
                break;
            }
            case ("nvarchar"): {
                baseType = -9;
                break;
            }
            case ("other"): {
                baseType = 1111;
                break;
            }
            case ("real"): {
                baseType = 7;
                break;
            }
            case ("ref"): {
                baseType = 2006;
                break;
            }
            case ("rowid"): {
                baseType = -8;
                break;
            }
            case ("smallint"): {
                baseType = 5;
                break;
            }
            case ("sqlxml"): {
                baseType = 2009;
                break;
            }
            case ("struct"): {
                baseType = 2002;
                break;
            }
            case ("time"): {
                baseType = 92;
                break;
            }
            case ("timestamp"): {
                baseType = 93;
                break;
            }
            case ("tinyint"): {
                baseType = -6;
                break;
            }
            case ("boolean"): {
                baseType = 16;
                break;
            }
            case ("varbinary"): {
                baseType = -3;
                break;
            }
            case ("varchar"): {
                baseType = 12;
            }
            default: {
                baseType=-1;
            }
        }
        return baseType;
    }
}
