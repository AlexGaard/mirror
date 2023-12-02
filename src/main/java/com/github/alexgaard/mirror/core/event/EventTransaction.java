package com.github.alexgaard.mirror.core.event;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class EventTransaction {

    public final UUID id;

    public final String sourceName;

    // When the transaction was committed
    public final long committedAt;

    // When this EventTransaction instance was created, should always be equal to or greater than committedAt.
    public final long createdAt;

    public final List<Event> events;

    public EventTransaction() {
        id = null;
        sourceName = null;
        committedAt = 0;
        createdAt = 0;
        events = Collections.emptyList();
    }

    public EventTransaction(UUID id, String sourceName, long committedAt, long createdAt, List<Event> events) {
        this.id = id;
        this.sourceName = sourceName;
        this.committedAt = committedAt;
        this.events = events;
        this.createdAt = createdAt;
    }

    public static EventTransaction of(String sourceName, List<Event> events) {
        return new EventTransaction(UUID.randomUUID(), sourceName, System.currentTimeMillis(), System.currentTimeMillis(), events);
    }

    public static EventTransaction of(String sourceName, Event event) {
        return of(sourceName, Collections.singletonList(event));
    }

}
