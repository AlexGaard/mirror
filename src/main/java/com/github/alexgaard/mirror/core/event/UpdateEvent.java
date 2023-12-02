package com.github.alexgaard.mirror.core.event;

import java.util.List;
import java.util.UUID;

public class UpdateEvent extends Event {

    public final static String TYPE = "delete";
    public final String namespace;

    public final String table;
    public final List<Field> identifier;

    public final List<Field> fields;

    public UpdateEvent(UUID id, long createdAt, String namespace, String table, List<Field> identifier, List<Field> fields) {
        super(id, TYPE, createdAt);
        this.namespace = namespace;
        this.table = table;
        this.identifier = identifier;
        this.fields = fields;
    }

}
