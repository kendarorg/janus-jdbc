package org.kendar.json;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.janus.JdbcResultSet;
import org.kendar.janus.utils.TestBase;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ResultSetConverterTest extends TestBase {

    @BeforeEach
    protected void beforeEach() throws SQLException {
        super.beforeEach();
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }

    public ArrayList<Object> fill(Object ... pars){
        var result = new ArrayList<Object>();
        for(var par:pars){
            result.add(par);
        }
        return  result;
    }

    @Test
    void testGetResultSet() throws Exception {
        var target = new ResultSetConverter();
        var connb = getConnection();
        connb.createStatement().execute("create table if not exists " +
                "persons(" +
                "   id IDENTITY NOT NULL PRIMARY KEY, " +
                "   name varchar(255)," +
                "   tst TIMESTAMP);");


        //insertInSimpleTable("test");
        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement();
        assertNotNull(statement);
        var result = statement.executeQuery("SELECT * FROM persons");

        var hamResultSet = target.toHam((JdbcResultSet) result);
        var newData = new ArrayList<List<Object>>();
        newData.add(fill(1L,"first",LocalDateTime.of(2010,1,1,1,1,1,1)));
        newData.add(fill(2L,"second", LocalDateTime.of(2011,1,1,1,1,1,1)));
        hamResultSet.fill(newData);
        var newRs = target.fromHam(hamResultSet);

        assertTrue(newRs.next());
        assertEquals("first",newRs.getString(2));
        assertEquals(1L,newRs.getLong("id"));
        assertEquals("2010-01-01 01:01:01.000000001",newRs.getTimestamp("tst").toString());

        assertTrue(newRs.next());
        assertEquals("second",newRs.getString(2));
        assertEquals(2L,newRs.getLong("id"));
        assertEquals("2011-01-01 01:01:01.000000001",newRs.getTimestamp("tst").toString());


        assertFalse(newRs.next());
        result.close();
        statement.close();
        conn.close();
    }



    @Test
    void testGetResultSetDate() throws Exception {
        var target = new ResultSetConverter();
        var connb = getConnection();
        connb.createStatement().execute("create table if not exists " +
                "persons(" +
                "   id IDENTITY NOT NULL PRIMARY KEY, " +
                "   name varchar(255)," +
                "   tst TIMESTAMP);");


        //insertInSimpleTable("test");
        var conn = driver.connect(CONNECT_URL,null);
        var statement = conn.createStatement();
        assertNotNull(statement);
        var result = statement.executeQuery("SELECT * FROM persons");

        var hamResultSet = target.toHam((JdbcResultSet) result);
        var newData = new ArrayList<List<Object>>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        newData.add(fill(1L,"first",format.parse ("2010-01-01")));
        newData.add(fill(2L,"second",format.parse ("2011-01-01")));
        hamResultSet.fill(newData);
        var newRs = target.fromHam(hamResultSet);

        assertTrue(newRs.next());
        assertEquals("first",newRs.getString(2));
        assertEquals(1L,newRs.getLong("id"));
        assertEquals("2010-01-01 00:00:00.0",newRs.getTimestamp("tst").toString());

        assertTrue(newRs.next());
        assertEquals("second",newRs.getString(2));
        assertEquals(2L,newRs.getLong("id"));
        assertEquals("2011-01-01 00:00:00.0",newRs.getTimestamp("tst").toString());


        assertFalse(newRs.next());
        result.close();
        statement.close();
        conn.close();
    }
}
