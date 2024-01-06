package com.github.alexgaard.mirror.postgres.event;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class DeleteEvent extends PostgresEvent {

    public final static String TYPE = "delete";

    public final String namespace;

    public final String table;

    public final List<Field<?>> identifierFields;

    // Used for deserialization
    public DeleteEvent() {
        super(null, TYPE, -1);
        this.namespace = null;
        this.table = null;
        this.identifierFields = null;
    }

    public DeleteEvent(UUID id, String namespace, String table, int transactionId, List<Field<?>> identifier) {
        super(id, TYPE, transactionId);
        this.namespace = namespace;
        this.table = table;
        this.identifierFields = identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeleteEvent that = (DeleteEvent) o;

        if (!Objects.equals(namespace, that.namespace)) return false;
        if (!Objects.equals(table, that.table)) return false;
        return Objects.equals(identifierFields, that.identifierFields);
    }

    @Override
    public int hashCode() {
        int result = namespace != null ? namespace.hashCode() : 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (identifierFields != null ? identifierFields.hashCode() : 0);
        return result;
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
