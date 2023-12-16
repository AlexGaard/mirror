package com.github.alexgaard.mirror.core.event;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class UpdateEvent extends Event {

    public final static String TYPE = "delete";
    public final String namespace;

    public final String table;
    public final List<Field> identifyingFields;

    public final List<Field> updateFields;

    public UpdateEvent(UUID id, OffsetDateTime createdAt, String namespace, String table, List<Field> identifyingFields, List<Field> updateFields) {
        super(id, TYPE, createdAt);
        this.namespace = namespace;
        this.table = table;
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
