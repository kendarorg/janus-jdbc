package org.kendar.janus.hibernate;

import javax.persistence.*;

@Entity
@Table(name="SIMPLE_TABLE")
public class SimpleTable {
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;


    @Column(name = "value")
    private String value;
}
