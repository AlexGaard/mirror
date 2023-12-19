package com.github.alexgaard.mirror.postgres.event;


import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class UpdateEvent extends PostgresEvent {

    public final static String TYPE = "update";

    public final List<Field<?>> identifierFields;

    public final List<Field<?>> fields;

    // Used for deserialization
    public UpdateEvent() {
        super(null, TYPE, null, null, -1, null);
        this.identifierFields = null;
        this.fields = null;
    }

    public UpdateEvent(UUID id, String namespace, String table, int transactionId, List<Field<?>> identifierFields, List<Field<?>> fields, OffsetDateTime createdAt) {
        super(id, TYPE, namespace, table, transactionId, createdAt);
        this.identifierFields = identifierFields;
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "UpdateEvent{" +
                "namespace='" + namespace + '\'' +
                ", table='" + table + '\'' +
                ", identifierFields=" + identifierFields +
                ", fields=" + fields +
                ", id=" + id +
                ", type='" + type + '\'' +
                '}';
    }
}
