package com.github.alexgaard.mirror.postgres.event;


import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class UpdateEvent extends PostgresEvent {

    public final static String TYPE = "update";

    public final List<Field<?>> identifyingFields;

    public final List<Field<?>> updateFields;

    // Used for deserialization
    public UpdateEvent() {
        super(null, TYPE, null, null, -1, null);
        this.identifyingFields = null;
        this.updateFields = null;
    }

    public UpdateEvent(UUID id, String namespace, String table, int transactionId, List<Field<?>> identifyingFields, List<Field<?>> updateFields, OffsetDateTime createdAt) {
        super(id, TYPE, namespace, table, transactionId, createdAt);
        this.identifyingFields = identifyingFields;
        this.updateFields = updateFields;
    }

    @Override
    public String toString() {
        return "UpdateEvent{" +
                "namespace='" + namespace + '\'' +
                ", table='" + table + '\'' +
                ", identifyingFields=" + identifyingFields +
                ", updateFields=" + updateFields +
                ", id=" + id +
                ", type='" + type + '\'' +
                '}';
    }
}
