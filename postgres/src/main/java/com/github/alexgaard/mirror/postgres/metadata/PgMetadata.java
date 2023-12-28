package com.github.alexgaard.mirror.postgres.metadata;

import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.core.exception.NotYetImplementedException;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import static com.github.alexgaard.mirror.postgres.utils.QueryUtils.*;
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

    public static Map<String, List<ColumnMetadata>> getColumnMetadataForTables(DataSource dataSource, String schema) {
        String sql = "select con.*\n" +
                "from pg_catalog.pg_constraint con\n" +
                "         inner join pg_catalog.pg_class rel\n" +
                "                    on rel.oid = con.conrelid\n" +
                "         inner join pg_catalog.pg_namespace nsp\n" +
                "                    on nsp.oid = connamespace\n" +
                "where nsp.nspname = ?";

        Map<String, List<ColumnMetadata>> metadata = new HashMap<>();

        query(dataSource, sql, statement -> {
            statement.setString(1, schema);

            ResultSet resultSet = statement.executeQuery();

            resultForEach(resultSet, (rs) -> {
                String table = rs.getString("table_name");

                List<ColumnMetadata> columns = metadata.computeIfAbsent(table, (ignored) -> new ArrayList<>());

                columns.add(new ColumnMetadata(rs.getString("column_name"), rs.getInt("ordinal_position"), "YES".equals(rs.getString("is_nullable"))));
            });
        });

        return metadata;
    }

    public static class ColumnMetadata {

        public final String name;

        public final int ordinalPosition;

        public final boolean isNullable;

        public ColumnMetadata(String name, int ordinalPosition, boolean isNullable) {
            this.name = name;
            this.ordinalPosition = ordinalPosition;
            this.isNullable = isNullable;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ColumnMetadata that = (ColumnMetadata) o;

            if (ordinalPosition != that.ordinalPosition) return false;
            if (isNullable != that.isNullable) return false;
            return name.equals(that.name);
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + ordinalPosition;
            result = 31 * result + (isNullable ? 1 : 0);
            return result;
        }
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

}
