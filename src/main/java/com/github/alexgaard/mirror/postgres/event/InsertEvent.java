package com.github.alexgaard.mirror.postgres.event;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class InsertEvent extends PostgresEvent {

    public final static String TYPE = "insert";

    public final List<Field<?>> fields;

    // Used for deserialization
    public InsertEvent() {
        super(null, TYPE, null, null, -1, null);
        this.fields = null;
    }

    public InsertEvent(UUID id, String namespace, String table, int transactionId, List<Field<?>> fields, OffsetDateTime createdAt) {
        super(id, TYPE, namespace, table, transactionId, createdAt);
        this.fields = fields;
    }

}
