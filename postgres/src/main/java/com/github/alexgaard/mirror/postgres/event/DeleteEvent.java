package com.github.alexgaard.mirror.postgres.event;

import com.github.alexgaard.mirror.core.event.Event;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class DeleteEvent extends PostgresEvent {

    public final static String TYPE = "delete";

    public final List<Field<?>> identifyingFields;

    // Used for deserialization
    public DeleteEvent() {
        super(null, TYPE, null, null, -1, null);
        this.identifyingFields = null;
    }

    public DeleteEvent(UUID id, String namespace, String table, int transactionId, List<Field<?>> identifier, OffsetDateTime createdAt) {
        super(id, TYPE, namespace, table, transactionId, createdAt);
        this.identifyingFields = identifier;
    }

}
