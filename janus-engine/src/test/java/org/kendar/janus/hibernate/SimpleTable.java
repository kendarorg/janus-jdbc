package org.kendar.janus.hibernate;

import javax.persistence.*;

@Entity
@Table(name="SIMPLE_TABLE")
public class SimpleTable {
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id")
    private long id;


    @Column(name = "content")
    private String content;
}
