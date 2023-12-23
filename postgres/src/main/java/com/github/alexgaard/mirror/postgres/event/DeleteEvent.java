package com.github.alexgaard.mirror.postgres.event;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class DeleteEvent extends DataChangeEvent {

    public final static String TYPE = "delete";

    public final List<Field<?>> identifierFields;

    // Used for deserialization
    public DeleteEvent() {
        super(null, TYPE, null, null, -1);
        this.identifierFields = null;
    }

    public DeleteEvent(UUID id, String namespace, String table, int transactionId, List<Field<?>> identifier) {
        super(id, TYPE, namespace, table, transactionId);
        this.identifierFields = identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeleteEvent that = (DeleteEvent) o;

        return Objects.equals(identifierFields, that.identifierFields);
    }

    @Override
    public int hashCode() {
        return identifierFields != null ? identifierFields.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "DeleteEvent{" +
                "identifierFields=" + identifierFields +
                ", id=" + id +
                ", type='" + type + '\'' +
                ", namespace='" + namespace + '\'' +
                ", table='" + table + '\'' +
                ", transactionId=" + transactionId +
                '}';
    }
}
