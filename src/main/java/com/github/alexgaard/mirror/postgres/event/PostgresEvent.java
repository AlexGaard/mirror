package com.github.alexgaard.mirror.postgres.event;

import com.github.alexgaard.mirror.core.event.Event;

import java.time.OffsetDateTime;
import java.util.UUID;

public abstract class PostgresEvent extends Event {

    public final String namespace;

    public final String table;

    public final int transactionId;

    public PostgresEvent(UUID id, String type, String namespace, String table, int transactionId, OffsetDateTime createdAt) {
        super(id, type, createdAt);
        this.namespace = namespace;
        this.table = table;
        this.transactionId = transactionId;
    }

}
