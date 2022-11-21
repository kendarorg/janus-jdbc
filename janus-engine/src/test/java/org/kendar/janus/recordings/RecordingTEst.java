package org.kendar.janus.recordings;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.janus.hibernate.SimpleTable;
import org.kendar.janus.utils.TestBase;

import javax.persistence.EntityManager;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordingTEst extends TestBase {

    @BeforeEach
    protected void beforeEach() throws SQLException {
        super.beforeEach();
        super.setupHibernate(SimpleTable.class);
    }
    @AfterEach
    protected void afterEach() throws SQLException {
        super.afterEach();
    }



    @Test
    public void savepointRollback() throws SQLException {

        var dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        String createStatement = "create table if not exists emp(id bigint NOT NULL PRIMARY KEY, name varchar(255), job varchar(255))";

        Statement stmt = dbConnection.createStatement();
        stmt.executeUpdate(createStatement);

        var id = serverEngine.startRecording();
        executeSavepointProcedure();
        serverEngine.stopRecording(id);
        dbConnection.createStatement().executeUpdate("DROP TABLE emp");
        System.out.println("------------------------------------------");
        System.out.println("------------------------------------------");
        System.out.println("------------------------------------------");
        serverEngine.startReplaying(id);
        executeSavepointProcedure();
        serverEngine.stopReplaying(id);
    }

    private void executeSavepointProcedure() throws SQLException {
        Statement stmt;
        var conn = driver.connect(CONNECT_URL,null);

        stmt = conn.createStatement();
        String query1 = "insert into emp values(5,'name','job')";
        String query2 = "select * from emp";

        conn.setAutoCommit(false);
        Savepoint spt1 = conn.setSavepoint("svpt1");
        stmt.execute(query1);
        ResultSet rs = stmt.executeQuery(query2);
        int no_of_rows = 0;

        while (rs.next()) {
            no_of_rows++;
        }
        assertEquals(1, no_of_rows);
        conn.rollback(spt1);
        conn.commit();
        no_of_rows = 0;
        rs = stmt.executeQuery(query2);

        while (rs.next()) {
            no_of_rows++;
        }
        assertEquals(0, no_of_rows);
    }

    @Test
    public void simpleTestHibernate() throws SQLException {

        var dbConnection = DriverManager.getConnection("jdbc:h2:mem:test;", "sa", "sa");
        var em = sessionFactory.createEntityManager();
        var id = serverEngine.startRecording();
        hibernateOperation(em);
        serverEngine.stopRecording(id);
        dbConnection.createStatement().executeUpdate("DROP TABLE SIMPLE_TABLE");
        System.out.println("------------------------------------------");
        System.out.println("------------------------------------------");
        System.out.println("------------------------------------------");
        serverEngine.startReplaying(id);
        var em2 = sessionFactory.createEntityManager();
        hibernateOperation(em2);
        serverEngine.stopReplaying(id);
    }

    private void hibernateOperation(EntityManager em) {
        em.getTransaction().begin();
        var st = new SimpleTable();
        st.setContent("TEST");
        em.persist(st);
        em.getTransaction().commit();
        em.close();

        var em2 = sessionFactory.createEntityManager();
        var results = em2.createQuery("SELECT e FROM SimpleTable e WHERE e.content='TEST'").getResultList();
        assertEquals(1,results.size());
    }
}
