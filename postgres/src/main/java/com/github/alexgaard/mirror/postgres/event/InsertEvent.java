package com.github.alexgaard.mirror.postgres.event;

import java.util.List;
import java.util.UUID;

public class InsertEvent extends DataChangeEvent {

    public final static String TYPE = "insert";

    public final List<Field<?>> fields;

    // Used for deserialization
    public InsertEvent() {
        super(null, TYPE, null, null, -1);
        this.fields = null;
    }

    public InsertEvent(UUID id, String namespace, String table, int transactionId, List<Field<?>> fields) {
        super(id, TYPE, namespace, table, transactionId);
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "InsertEvent{" +
                "fields=" + fields +
                ", id=" + id +
                ", type='" + type + '\'' +
                ", namespace='" + namespace + '\'' +
                ", table='" + table + '\'' +
                ", transactionId=" + transactionId +
                '}';
    }
}
