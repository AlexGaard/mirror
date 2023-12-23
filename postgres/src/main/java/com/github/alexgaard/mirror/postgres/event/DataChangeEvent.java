package com.github.alexgaard.mirror.postgres.event;

import java.util.UUID;

public abstract class DataChangeEvent {

    public final UUID id;

    public final String type;
    public final String namespace;

    public final String table;

    public final int transactionId;

    public DataChangeEvent(UUID id, String type, String namespace, String table, int transactionId) {
        this.id = id;
        this.type = type;
        this.namespace = namespace;
        this.table = table;
        this.transactionId = transactionId;
    }

}
