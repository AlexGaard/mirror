package com.github.alexgaard.mirror.postgres.utils;

import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.core.exception.NotYetImplementedException;

import java.sql.Types;

import static java.lang.String.format;

public class SqlFieldType {

    public static int sqlFieldType(Field<?> field) {
        if (field.value == null) {
            return Types.NULL;
        }

        switch (field.type) {
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case INT16:
                return Types.SMALLINT;
            case INT32:
                return Types.INTEGER;
            case INT64:
                return Types.BIGINT;
            case CHAR:
                return Types.CHAR;
            case BOOLEAN:
                return Types.BOOLEAN;
            case TEXT:
            case JSON:
            case JSONB:
            case UUID:
                return Types.VARCHAR;
            case BYTES:
                return Types.BINARY;
            case DATE:
                return Types.DATE;
            case TIME:
                return Types.TIME;
            case TIMESTAMP:
                return Types.TIMESTAMP;
            case TIMESTAMP_TZ:
                return Types.TIMESTAMP_WITH_TIMEZONE;
            default:
                throw new NotYetImplementedException(format("No mapping to SQL type for field '%s' with type '%s'", field.name, field.type));
        }
    }

}
