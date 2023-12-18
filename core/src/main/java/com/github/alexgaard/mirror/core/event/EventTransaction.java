package com.github.alexgaard.mirror.core.event;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class EventTransaction {

    public final UUID id;

    public final String sourceName;

    public final List<Event> events;

    // When the transaction was committed
    public final OffsetDateTime committedAt;

    // When this EventTransaction instance was created, should always be equal to or greater than committedAt.
    public final OffsetDateTime createdAt;

    public EventTransaction() {
        id = null;
        sourceName = null;
        events = null;
        committedAt = null;
        createdAt = null;
    }

    public EventTransaction(UUID id, String sourceName, List<Event> events, OffsetDateTime committedAt, OffsetDateTime createdAt) {
        this.id = id;
        this.sourceName = sourceName;
        this.events = events;
        this.committedAt = committedAt;
        this.createdAt = createdAt;
    }

    public static EventTransaction of(String sourceName, List<Event> events) {
        var now = OffsetDateTime.now();
        return new EventTransaction(UUID.randomUUID(), sourceName, events, now, now);
    }

    public static EventTransaction of(String sourceName, Event event) {
        return of(sourceName, Collections.singletonList(event));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventTransaction that = (EventTransaction) o;
        return Objects.equals(id, that.id) && Objects.equals(sourceName, that.sourceName) && Objects.equals(events, that.events) && Objects.equals(committedAt, that.committedAt) && Objects.equals(createdAt, that.createdAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, sourceName, events, committedAt, createdAt);
    }

    @Override
    public String toString() {
        return "EventTransaction{" +
                "id=" + id +
                ", sourceName='" + sourceName + '\'' +
                ", events=" + events +
                ", committedAt=" + committedAt +
                ", createdAt=" + createdAt +
                '}';
    }
}
