package com.github.alexgaard.mirror.postgres.event;

import com.github.alexgaard.mirror.core.Event;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class PostgresTransactionEvent extends Event {

    public final static String TYPE = "postgres-transaction";

    public final List<DataChangeEvent> events;

    public final OffsetDateTime commitedAt;

    public PostgresTransactionEvent() {
        super(null, null, null, null);
        this.events = null;
        this.commitedAt = null;
    }

    public PostgresTransactionEvent(UUID id, String sourceName, String type, List<DataChangeEvent> events, OffsetDateTime commitedAt) {
        super(id, sourceName, type, commitedAt);
        this.events = events;
        this.commitedAt = commitedAt;
    }

    public static PostgresTransactionEvent of(String sourceName, List<DataChangeEvent> events) {
        return new PostgresTransactionEvent(UUID.randomUUID(), sourceName, TYPE, events, OffsetDateTime.now());
    }

    public static PostgresTransactionEvent of(String sourceName, DataChangeEvent event) {
        return of(sourceName, Collections.singletonList(event));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PostgresTransactionEvent that = (PostgresTransactionEvent) o;

        if (!Objects.equals(events, that.events)) return false;
        return Objects.equals(commitedAt, that.commitedAt);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (events != null ? events.hashCode() : 0);
        result = 31 * result + (commitedAt != null ? commitedAt.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PostgresTransactionEvent{" +
                "events=" + events +
                ", commitedAt=" + commitedAt +
                ", id=" + id +
                ", eventType='" + type + '\'' +
                ", sourceName='" + sourceName + '\'' +
                ", createdAt=" + createdAt +
                '}';
    }
}
