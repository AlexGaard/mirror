package com.github.alexgaard.mirror.core;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

public class Event {

    public final UUID id;

    public final String type;

    public final String sourceName;

    public final OffsetDateTime createdAt;

    public Event() {
        id = null;
        type = null;
        sourceName = null;
        createdAt = null;
    }

    public Event(UUID id, String sourceName, String type, OffsetDateTime createdAt) {
        this.id = id;
        this.sourceName = sourceName;
        this.type = type;
        this.createdAt = createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(id, event.id) && Objects.equals(type, event.type) && Objects.equals(sourceName, event.sourceName) && Objects.equals(createdAt, event.createdAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, sourceName, createdAt);
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", type='" + type + '\'' +
                ", sourceName='" + sourceName + '\'' +
                ", createdAt=" + createdAt +
                '}';
    }
}
