package com.github.alexgaard.mirror.postgres.utils;

import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.core.exception.NotYetImplementedException;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class PgMetadata {

    /*
        Retrieves all pg data types.
        Map<data oid, data type>
    */
    public static Map<Integer, PgDataType> getAllPgDataTypes(DataSource dataSource) {
        Map<Integer, PgDataType> pgDataTypes = new HashMap<>();

        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select oid, typname from pg_type");

                while (resultSet.next()) {
                    Integer oid = resultSet.getInt(1);
                    String typename = resultSet.getString(2);

                    pgDataTypes.put(oid, new PgDataType(typename));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return pgDataTypes;
    }

    public static class PgDataType {
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
                case "jsonb":
                    return Field.Type.JSON;
                case "varchar":
                case "text":
                    return Field.Type.STRING;
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

}
