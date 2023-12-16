package com.github.alexgaard.mirror.core.event;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class InsertEvent extends Event {

    public final static String TYPE = "insert";

    public final String namespace;

    public final String table;

    public final List<Field> insertFields;

    public InsertEvent() {
        super(null, TYPE, null);
        this.namespace = null;
        this.table = null;
        this.insertFields = null;
    }

    public InsertEvent(UUID id, OffsetDateTime createdAt, String namespace, String table, List<Field> fields) {
        super(id, TYPE, createdAt);
        this.namespace = namespace;
        this.table = table;
        this.insertFields = fields;
    }

}
