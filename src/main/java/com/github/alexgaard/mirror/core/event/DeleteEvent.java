package com.github.alexgaard.mirror.core.event;

import java.util.List;
import java.util.UUID;

public class DeleteEvent extends Event {

    public final static String TYPE = "delete";
    public final String namespace;

    public final String table;
    public final List<Field> identifyingFields;

    public DeleteEvent(UUID id, String namespace, String table, List<Field> identifyingFields) {
        super(id, TYPE);
        this.namespace = namespace;
        this.table = table;
        this.identifyingFields = identifyingFields;
    }

}
