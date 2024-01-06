package com.github.alexgaard.mirror.postgres.event;


import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class UpdateEvent extends PostgresEvent {

    public final static String TYPE = "update";

    public final String namespace;


    public final String table;

    public final List<Field<?>> identifierFields;

    public final List<Field<?>> fields;

    // Used for deserialization
    public UpdateEvent() {
        super(null, TYPE, -1);
        this.namespace = null;
        this.table = null;
        this.identifierFields = null;
        this.fields = null;
    }

    public UpdateEvent(UUID id, String namespace, String table, int transactionId, List<Field<?>> identifierFields, List<Field<?>> fields) {
        super(id, TYPE, transactionId);
        this.namespace = namespace;
        this.table = table;
        this.identifierFields = identifierFields;
        this.fields = fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateEvent that = (UpdateEvent) o;

        if (!Objects.equals(namespace, that.namespace)) return false;
        if (!Objects.equals(table, that.table)) return false;
        if (!Objects.equals(identifierFields, that.identifierFields))
            return false;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = namespace != null ? namespace.hashCode() : 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (identifierFields != null ? identifierFields.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "UpdateEvent{" +
                "identifierFields=" + identifierFields +
                ", fields=" + fields +
                ", id=" + id +
                ", type='" + type + '\'' +
                ", namespace='" + namespace + '\'' +
                ", table='" + table + '\'' +
                ", transactionId=" + transactionId +
                '}';
    }
}
