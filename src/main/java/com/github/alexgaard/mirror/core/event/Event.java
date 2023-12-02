package com.github.alexgaard.mirror.core.event;

import java.util.Objects;
import java.util.UUID;

public abstract class Event {

    public final UUID id;

    public final String type;

    protected Event(UUID id, String type) {
        this.id = id;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event that = (Event) o;
        return Objects.equals(id, that.id) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type);
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", type='" + type + '\'' +
                '}';
    }
}
