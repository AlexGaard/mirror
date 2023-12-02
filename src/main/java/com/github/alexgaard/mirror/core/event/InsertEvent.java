package com.github.alexgaard.mirror.core.event;

import java.util.List;
import java.util.UUID;

public class InsertEvent extends Event {

    public final static String TYPE = "insert";

    public final String namespace;

    public final String table;

    public final List<Field> fields;

    public InsertEvent(UUID id, String namespace, String table, List<Field> fields) {
        super(id, TYPE);
        this.namespace = namespace;
        this.table = table;
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "InsertDataChange{" +
                "namespace='" + namespace + '\'' +
                ", table='" + table + '\'' +
                ", fields=" + fields +
                ", id=" + id +
                ", type='" + type + '\'' +
                '}';
    }
}
