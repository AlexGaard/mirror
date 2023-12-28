package com.github.alexgaard.mirror.postgres.collector;

import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;
import static com.github.alexgaard.mirror.postgres.utils.QueryUtils.*;
import static java.lang.String.format;

public class PgReplication {

    private final static String DEFAULT_SCHEMA = "public";

    private final Logger log = LoggerFactory.getLogger(PgReplication.class);

    private final Map<String, Set<String>> schemaAndTableExclusions = new HashMap<>();

    private String replicationSlotName = "mirror";

    private String publicationName = "mirror";

    public PgReplication publicationName(String publicationName) {
        this.publicationName = publicationName;
        return this;
    }

    public PgReplication replicationSlotName(String replicationSlotName) {
        this.replicationSlotName = replicationSlotName;
        return this;
    }

    public PgReplication allTables() {
        return allTables(DEFAULT_SCHEMA);
    }

    public PgReplication allTables(String schema) {
        schemaAndTableExclusions.put(schema, new HashSet<>());
        return this;
    }

    public PgReplication exclude(String tableName) {
        return exclude(DEFAULT_SCHEMA, tableName);
    }

    public PgReplication exclude(String schema, String tableName) {
        Set<String> exclusions = schemaAndTableExclusions.computeIfAbsent(schema, (s) -> new HashSet<>());
        exclusions.add(tableName);
        return this;
    }

    public String getReplicationSlotName() {
        return replicationSlotName;
    }

    public String getPublicationName() {
        return publicationName;
    }

    public Set<String> getSchemas() {
        return schemaAndTableExclusions.keySet();
    }

    public synchronized void setup(DataSource dataSource) {
        if (!hasReplicationSlot(dataSource, replicationSlotName)) {
            log.info("Creating new replication slot {}", replicationSlotName);
            createReplicationSlot(dataSource, replicationSlotName);
        }

        if (!hasPublication(dataSource, publicationName)) {
            log.info("Creating new publication {}", publicationName);
            createPublication(dataSource, publicationName);
        }

        schemaAndTableExclusions.forEach((schema, exclusions) -> {
            List<String> includedTables = getAllTables(dataSource, schema)
                    .stream()
                    .filter(t -> !exclusions.contains(t))
                    .collect(Collectors.toList());


            List<String> publicationTables = getTablesForPublication(dataSource, publicationName, schema);

            List<String> tablesToAddToPublication = includedTables
                    .stream()
                    .filter(t -> !publicationTables.contains(t))
                    .collect(Collectors.toList());

            List<String> tablesToRemoveFromPublication = publicationTables
                    .stream()
                    .filter(t -> !includedTables.contains(t))
                    .collect(Collectors.toList());

            List<String> tablesWithoutFullReplicaIdentity = getTablesWithoutFullReplicaIdentity(dataSource, schema, includedTables);

            tablesToAddToPublication.forEach(table -> {
                log.info("Adding new table {}.{} to publication {}", schema, table, publicationName);
                addTableToPublication(dataSource, publicationName, schema, table);
            });

            tablesToRemoveFromPublication.forEach(table -> {
                log.info("Removing table {}.{} from publication {}", schema, table, publicationName);
                removeTableFromPublication(dataSource, publicationName, schema, table);
            });

            tablesWithoutFullReplicaIdentity.forEach(table -> {
                log.info("Setting replica identity to FULL for {}.{}", schema, table);
                setReplicaIdentityFull(dataSource, schema, table);
            });
        });
    }

    private static boolean hasReplicationSlot(DataSource dataSource, String replicationSlotName) {
        String sql = "select 1 from pg_replication_slots where slot_name = ?";

        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, replicationSlotName);
                ResultSet resultSet = statement.executeQuery();
                return resultSet.next();
            }
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

    private static void createReplicationSlot(DataSource dataSource, String replicationSlotName) {
        String sql = "select pg_create_logical_replication_slot(?, 'pgoutput')";

        query(dataSource, sql, (statement -> {
            statement.setString(1, replicationSlotName);
            return statement.execute();
        }));
    }

    private static boolean hasPublication(DataSource dataSource, String publicationName) {
        String sql = "select 1 from pg_publication where pubname = ?";

        return query(dataSource, sql, (statement) -> {
            statement.setString(1, publicationName);
            ResultSet resultSet = statement.executeQuery();
            return resultSet.next();
        });
    }

    private static void createPublication(DataSource dataSource, String publicationName) {
        String sql = format("CREATE PUBLICATION %s", publicationName);

        update(dataSource, sql);
    }

    private static List<String> getAllTables(DataSource dataSource, String schema) {
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? and table_type = 'BASE TABLE'";

        return query(dataSource, sql, (statement) -> {
            statement.setString(1, schema);

            ResultSet resultSet = statement.executeQuery();

            return resultList(resultSet, (rs) -> rs.getString(1));
        });
    }

    private static List<String> getTablesForPublication(DataSource dataSource, String publicationName, String schema) {
        String sql = "select tablename from pg_publication_tables WHERE pubname = ? AND schemaname = ?";

        return query(dataSource, sql, (statement) -> {
            statement.setString(1, publicationName);
            statement.setString(2, schema);

            ResultSet resultSet = statement.executeQuery();

            return resultList(resultSet, (rs) -> rs.getString(1));
        });
    }

    private static List<String> getTablesWithoutFullReplicaIdentity(DataSource dataSource, String schema, List<String> tables) {
        String tablesSql = tables.stream().map(table -> format("'%s.%s'::regclass", schema, table))
                .collect(Collectors.joining(", "));

        String sql = format("select relname from pg_class where relreplident != 'f' and oid in (%s)", tablesSql);

        return query(dataSource, sql, (statement) -> {
            ResultSet resultSet = statement.executeQuery();
            return resultList(resultSet, (rs) -> rs.getString(1));
        });
    }

    private static void setReplicaIdentityFull(DataSource dataSource, String schema, String table) {
        String sql = format("alter table %s.%s replica identity full", schema, table);

        update(dataSource, sql);
    }

    private static void addTableToPublication(DataSource dataSource, String publicationName, String schema, String table) {
        String sql = format("ALTER PUBLICATION %s ADD TABLE %s.%s", publicationName, schema, table);

        update(dataSource, sql);
    }

    private static void removeTableFromPublication(DataSource dataSource, String publicationName, String schema, String table) {
        String sql = format("ALTER PUBLICATION %s DROP TABLE %s.%s", publicationName, schema, table);

        update(dataSource, sql);
    }

}
