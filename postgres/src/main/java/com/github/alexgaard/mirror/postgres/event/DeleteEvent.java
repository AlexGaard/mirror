package com.github.alexgaard.mirror.postgres.event;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class DeleteEvent extends PostgresEvent {

    public final static String TYPE = "delete";

    public final List<Field<?>> identifierFields;

    // Used for deserialization
    public DeleteEvent() {
        super(null, TYPE, null, null, -1, null);
        this.identifierFields = null;
    }

    public DeleteEvent(UUID id, String namespace, String table, int transactionId, List<Field<?>> identifier, OffsetDateTime createdAt) {
        super(id, TYPE, namespace, table, transactionId, createdAt);
        this.identifierFields = identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        DeleteEvent that = (DeleteEvent) o;
        return Objects.equals(identifierFields, that.identifierFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), identifierFields);
    }

    @Override
    public String toString() {
        return "DeleteEvent{" +
                "identifierFields=" + identifierFields +
                ", namespace='" + namespace + '\'' +
                ", table='" + table + '\'' +
                ", transactionId=" + transactionId +
                ", id=" + id +
                ", type='" + type + '\'' +
                ", createdAt=" + createdAt +
                '}';
    }
}
