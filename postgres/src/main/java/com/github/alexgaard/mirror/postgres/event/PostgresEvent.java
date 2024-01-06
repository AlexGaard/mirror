package com.github.alexgaard.mirror.postgres.event;

import java.util.UUID;

public abstract class PostgresEvent {

    public final UUID id;

    public final String type;

    public final int transactionId;

    public PostgresEvent(UUID id, String type, int transactionId) {
        this.id = id;
        this.type = type;
        this.transactionId = transactionId;
    }

}
