package com.github.alexgaard.mirror.postgres.event;

import java.util.Objects;
import java.util.UUID;

public class CustomMessageEvent extends PostgresEvent {
    public final static String TYPE = "custom-message";

    public final String prefix;

    public final String message;

    // Used for deserialization
    public CustomMessageEvent() {
        super(null, TYPE, -1);
        this.prefix = null;
        this.message = null;
    }

    public CustomMessageEvent(UUID id, String prefix, String message, int transactionId) {
        super(id, TYPE, transactionId);
        this.prefix = prefix;
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomMessageEvent that = (CustomMessageEvent) o;

        if (!Objects.equals(prefix, that.prefix)) return false;
        return Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        int result = prefix != null ? prefix.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CustomMessageEvent{" +
                "prefix='" + prefix + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
