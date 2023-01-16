package org.kendar.janus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.janus.hibernate.SimpleTable;
import org.kendar.janus.utils.TestBase;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HibernateTest  extends TestBase {

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
    public void simpleTest(){

        var em = sessionFactory.createEntityManager();
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
