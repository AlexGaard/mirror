package com.github.alexgaard.mirror.postgres.utils;

import com.github.alexgaard.mirror.core.exception.ParseException;

import java.util.Arrays;


public class TupleDataColumn {

    public enum Type {
        NULL,
        TOASTED,
        TEXT,
        BINARY;

        public static Type of(char typeChar) {
            switch (typeChar) {
                case 'n':
                    return NULL;
                case 'u':
                    return TOASTED;
                case 't':
                    return TEXT;
                case 'b':
                    return BINARY;
                default:
                    throw new ParseException("Invalid column type: " + typeChar);
            }
        }
    }

    public final Type type;

    private final byte[] data;

    public TupleDataColumn(Type type, byte[] data) {
        this.type = type;
        this.data = data;
    }

    public Object getData() {
        switch (type) {
            case TOASTED:
                throw new IllegalStateException("Unable to handle toasted values");
            case NULL:
                return null;
            case TEXT:
                return new String(data);
            case BINARY:
                return data;
            default:
                throw new ParseException("Unknown tuple data column type " + type);
        }
    }

    @Override
    public String toString() {
        return "TupleDataColumn{" +
                "type=" + type +
                ", data=" + (type.equals(Type.TEXT) && data != null ? new String(data) : Arrays.toString(data)) +
                '}';
    }
}
