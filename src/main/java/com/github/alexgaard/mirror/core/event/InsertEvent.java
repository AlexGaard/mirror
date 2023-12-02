package com.github.alexgaard.mirror.core.event;

import java.util.List;
import java.util.UUID;

public class InsertEvent extends Event {

    public final static String TYPE = "insert";

    public final String namespace;

    public final String table;

    public final List<Field> insertFields;

    public InsertEvent(UUID id, String namespace, String table, List<Field> insertFields) {
        super(id, TYPE);
        this.namespace = namespace;
        this.table = table;
        this.insertFields = insertFields;
    }

    @Override
    public String toString() {
        return "InsertDataChange{" +
                "namespace='" + namespace + '\'' +
                ", table='" + table + '\'' +
                ", insertFields=" + insertFields +
                ", id=" + id +
                ", type='" + type + '\'' +
                '}';
    }
}
