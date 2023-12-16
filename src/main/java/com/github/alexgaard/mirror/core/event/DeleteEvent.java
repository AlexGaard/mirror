package com.github.alexgaard.mirror.core.event;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class DeleteEvent extends Event {

    public final static String TYPE = "delete";
    public final String namespace;

    public final String table;
    public final List<Field> identifyingFields;

    public DeleteEvent(UUID id, OffsetDateTime createdAt, String namespace, String table, List<Field> identifier) {
        super(id, TYPE, createdAt);
        this.namespace = namespace;
        this.table = table;
        this.identifyingFields = identifier;
    }

}
