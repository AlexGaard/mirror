package com.github.alexgaard.mirror.postgres.metadata;

import com.github.alexgaard.mirror.core.exception.NotYetImplementedException;
import com.github.alexgaard.mirror.postgres.event.Field;

import java.util.Objects;

import static java.lang.String.format;

public class PgDataType {
    public final String typeName;

    public PgDataType(String typeName) {
        this.typeName = typeName;
    }

    public Field.Type getType() {
        switch (typeName) {
            case "int2":
                return Field.Type.INT16;
            case "int4":
                return Field.Type.INT32;
            case "int8":
                return Field.Type.INT64;
            case "float4":
                return Field.Type.FLOAT;
            case "float8":
                return Field.Type.DOUBLE;
            case "bool":
            case "boolean":
                return Field.Type.BOOLEAN;
            case "json":
                return Field.Type.JSON;
            case "jsonb":
                return Field.Type.JSONB;
            case "varchar":
            case "text":
                return Field.Type.TEXT;
            case "uuid":
                return Field.Type.UUID;
            case "bpchar":
                return Field.Type.CHAR;
            case "bytea":
                return Field.Type.BYTES;
            case "date":
                return Field.Type.DATE;
            case "timestamp":
                return Field.Type.TIMESTAMP;
            case "timestamptz":
                return Field.Type.TIMESTAMP_TZ;
            case "time":
                return Field.Type.TIME;
            default:
                throw new NotYetImplementedException(format("The type '%s' is not yet implemented", typeName));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PgDataType that = (PgDataType) o;
        return Objects.equals(typeName, that.typeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName);
    }

    @Override
    public String toString() {
        return "PgDataType{" +
                "typeName='" + typeName + '\'' +
                '}';
    }
}