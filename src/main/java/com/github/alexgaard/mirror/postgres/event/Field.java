package com.github.alexgaard.mirror.postgres.event;

import java.util.Objects;

public class Field {

    public enum Type {
        FLOAT,
        DOUBLE,
        BOOLEAN,
        STRING,
        JSON,
        UUID,
        CHAR,
        INT16,
        INT32,
        INT64,
        BYTES,
        DATE,
        TIME,
        TIMESTAMP,
        TIMESTAMP_TZ,
        NULL,
    }

    public final String name;

    public final Type type;
    public final Object value;

    public Field() {
        this.name = null;
        this.type = null;
        this.value = null;
    }

    public Field(String name, Type type, Object value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return Objects.equals(name, field.name) && Objects.equals(value, field.value) && type == field.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, type);
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", value=" + value +
                '}';
    }

}
