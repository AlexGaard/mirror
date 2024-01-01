package com.github.alexgaard.mirror.postgres.event;

import com.github.alexgaard.mirror.core.exception.NotYetImplementedException;

public enum FieldType {
    NOT_IMPLEMENTED,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    TEXT,
    JSON,
    JSONB,

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

    FLOAT_ARRAY,
    DOUBLE_ARRAY,
    BOOLEAN_ARRAY,
    TEXT_ARRAY,
    UUID_ARRAY,
    CHAR_ARRAY,
    INT16_ARRAY,
    INT32_ARRAY,
    INT64_ARRAY,
    DATE_ARRAY,
    TIME_ARRAY,
    TIMESTAMP_ARRAY,
    TIMESTAMP_TZ_ARRAY;

    public boolean isArray() {
        return this.name().endsWith("_ARRAY");
    }

    public String toBasePgType() {
        String baseTypeName = this.name().replace("_ARRAY", "");

        switch (FieldType.valueOf(baseTypeName)) {
            case FLOAT:
                return "float4";
            case DOUBLE:
                return "float8";
            case BOOLEAN:
                return "boolean";
            case TEXT:
                return "text";
            case JSON:
                return "json";
            case JSONB:
                return "jsonb";
            case UUID:
                return "uuid";
            case CHAR:
                return "bpchar";
            case INT16:
                return "int2";
            case INT32:
                return "int4";
            case INT64:
                return "int8";
            case BYTES:
                return "bytea";
            case DATE:
                return "date";
            case TIME:
                return "time";
            case TIMESTAMP:
                return "timestamp";
            case TIMESTAMP_TZ:
                return "timestamptz";
            default:
                throw new NotYetImplementedException("Missing implementation for " + this);
        }
    }

    public static FieldType ofPgType(String type) {
        switch (type) {
            case "int2":
                return FieldType.INT16;
            case "int4":
                return FieldType.INT32;
            case "int8":
                return FieldType.INT64;
            case "float4":
                return FieldType.FLOAT;
            case "float8":
                return FieldType.DOUBLE;
            case "bool":
            case "boolean":
                return FieldType.BOOLEAN;
            case "json":
                return FieldType.JSON;
            case "jsonb":
                return FieldType.JSONB;
            case "varchar":
            case "text":
                return FieldType.TEXT;
            case "uuid":
                return FieldType.UUID;
            case "bpchar":
                return FieldType.CHAR;
            case "bytea":
                return FieldType.BYTES;
            case "date":
                return FieldType.DATE;
            case "timestamp":
                return FieldType.TIMESTAMP;
            case "timestamptz":
                return FieldType.TIMESTAMP_TZ;
            case "time":
                return FieldType.TIME;
            // Array types
            case "_int2":
                return FieldType.INT16_ARRAY;
            case "_int4":
                return FieldType.INT32_ARRAY;
            case "_int8":
                return FieldType.INT64_ARRAY;
            case "_float4":
                return FieldType.FLOAT_ARRAY;
            case "_float8":
                return FieldType.DOUBLE_ARRAY;
            case "_bool":
            case "_boolean":
                return FieldType.BOOLEAN_ARRAY;
            case "_varchar":
            case "_text":
                return FieldType.TEXT_ARRAY;
            case "_uuid":
                return FieldType.UUID_ARRAY;
            case "_bpchar":
                return FieldType.CHAR_ARRAY;
            case "_date":
                return FieldType.DATE_ARRAY;
            case "_timestamp":
                return FieldType.TIMESTAMP_ARRAY;
            case "_timestamptz":
                return FieldType.TIMESTAMP_TZ_ARRAY;
            case "_time":
                return FieldType.TIME_ARRAY;
            default:
                return FieldType.NOT_IMPLEMENTED;
        }
    }
}
