package com.github.alexgaard.mirror.postgres.metadata;


import com.github.alexgaard.mirror.postgres.utils.ArrayUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;
import static com.github.alexgaard.mirror.postgres.utils.QueryUtils.*;

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
                    Integer oid = resultSet.getInt("oid");
                    String typename = resultSet.getString("typname");

                    pgDataTypes.put(oid, new PgDataType(typename));
                }
            }
        } catch (SQLException e) {
            throw softenException(e);
        }

        return pgDataTypes;
    }

    // Key = "<schema>.<table_name>"
    public static Map<String, List<ColumnMetadata>> getAllTableColumns(DataSource dataSource, String schema) {
        List<ColumnMetadata> tableColumns = getAllTableColumnsList(dataSource, schema);
        var map = new HashMap<String, List<ColumnMetadata>>();

        tableColumns.forEach(c -> {
            List<ColumnMetadata> columns = map.computeIfAbsent(tableFullName(c.schemaName, c.tableName), (ignored) -> new ArrayList<>());
            columns.add(c);
        });

        return map;
    }

    // Key = "<schema>.<table_name>"
    public static Map<String, List<ConstraintMetadata>> getAllTableConstraints(DataSource dataSource, String schema) {
        List<ConstraintMetadata> tableConstraints = getAllTableConstraintsList(dataSource, schema);
        var map = new HashMap<String, List<ConstraintMetadata>>();

        tableConstraints.forEach(c -> {
            List<ConstraintMetadata> constraints = map.computeIfAbsent(tableFullName(c.schemaName, c.tableName), (ignored) -> new ArrayList<>());
            constraints.add(c);
        });

        return map;
    }

    public static String tableFullName(String schema, String table) {
        return schema + "." + table;
    }

    private static List<ColumnMetadata> getAllTableColumnsList(DataSource dataSource, String schema) {
        String sql = "SELECT * FROM information_schema.columns WHERE table_schema = ?";

        return query(dataSource, sql, statement -> {
            statement.setString(1, schema);
            ResultSet resultSet = statement.executeQuery();

            return resultList(resultSet, (rs) -> new ColumnMetadata(
                    schema,
                    rs.getString("table_name"),
                    rs.getString("column_name"),
                    rs.getInt("ordinal_position"),
                    "YES".equals(rs.getString("is_nullable"))
            ));
        });
    }

    private static List<ConstraintMetadata> getAllTableConstraintsList(DataSource dataSource, String schema) {
        String sql = "select con.conname, rel.relname, con.contype, con.conkey\n" +
                "from pg_catalog.pg_constraint con\n" +
                "         inner join pg_catalog.pg_class rel\n" +
                "                    on rel.oid = con.conrelid\n" +
                "         inner join pg_catalog.pg_namespace nsp\n" +
                "                    on nsp.oid = connamespace\n" +
                "where nsp.nspname = ? order by con.contype, con.oid";

        return query(dataSource, sql, statement -> {
            statement.setString(1, schema);

            ResultSet resultSet = statement.executeQuery();

            return resultList(resultSet, (rs) -> new ConstraintMetadata(
                    schema,
                    rs.getString("relname"),
                    rs.getString("conname"),
                    toConstraintType(rs.getString("contype").charAt(0)),
                    ArrayUtils.toIntList((Short[]) rs.getArray("conkey").getArray())
            ));
        });
    }

    private static ConstraintMetadata.ConstraintType toConstraintType(char c) {
        switch (c) {
            case 'p':
                return ConstraintMetadata.ConstraintType.PRIMARY_KEY;
            case 'u':
                return ConstraintMetadata.ConstraintType.UNIQUE;
            default:
                return ConstraintMetadata.ConstraintType.OTHER;
        }
    }


}
