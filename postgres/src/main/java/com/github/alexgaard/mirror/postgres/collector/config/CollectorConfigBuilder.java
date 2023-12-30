package com.github.alexgaard.mirror.postgres.collector.config;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.*;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;
import static com.github.alexgaard.mirror.postgres.metadata.PgMetadata.tableFullName;
import static com.github.alexgaard.mirror.postgres.utils.QueryUtils.query;
import static com.github.alexgaard.mirror.postgres.utils.QueryUtils.resultList;

public class CollectorConfigBuilder {

    private final static String DEFAULT_SCHEMA = "public";

    public final static String DEFAULT_REPLICATION_SLOT_NAME = "mirror";

    public final static String DEFAULT_PUBLICATION_NAME = "mirror";

    private final CollectorConfig config = new CollectorConfig(null, DEFAULT_REPLICATION_SLOT_NAME, DEFAULT_PUBLICATION_NAME);

    private final DataSource dataSource;

    public CollectorConfigBuilder(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public CollectorConfigBuilder sourceName(String sourceName) {
        config.sourceName = sourceName;
        return this;
    }

    public CollectorConfigBuilder publicationName(String publicationName) {
        config.publicationName = publicationName;
        return this;
    }

    public CollectorConfigBuilder replicationSlotName(String replicationSlotName) {
        config.replicationSlotName = replicationSlotName;
        return this;
    }

    public CollectorConfigBuilder pollInterval(Duration pollInterval) {
        config.pollInterval = pollInterval;
        return this;
    }

    public CollectorConfigBuilder backoffIncrease(Duration backoffIncrease) {
        config.backoffIncrease = backoffIncrease;
        return this;
    }

    public CollectorConfigBuilder maxBackoff(Duration maxBackoff) {
        config.maxBackoff = maxBackoff;
        return this;
    }

    public CollectorConfigBuilder includeAll() {
        return includeAll(DEFAULT_SCHEMA);
    }

    public CollectorConfigBuilder includeAll(String schema) {
        config.schemaAndIncludedTables.put(schema, new HashSet<>(getAllTables(dataSource, schema)));
        return this;
    }

    public CollectorConfigBuilder include(String tableName) {
        return include(DEFAULT_SCHEMA, tableName);
    }

    public CollectorConfigBuilder include(String schema, String tableName) {
        Set<String> tables = config.schemaAndIncludedTables.computeIfAbsent(schema, (ignored) -> new HashSet<>());
        tables.add(tableName);

        return this;
    }

    public CollectorConfigBuilder exclude(String tableName) {
        return exclude(DEFAULT_SCHEMA, tableName);
    }

    public CollectorConfigBuilder exclude(String schema, String tableName) {
        Set<String> tables = config.schemaAndIncludedTables.get(schema);

        if (tables != null) {
            tables.remove(tableName);
        }

        return this;
    }

    public CollectorConfigBuilder configure(String schema, String tableName, CollectorTableConfig collectorTableConfig) {
        config.tableConfig.put(tableFullName(schema, tableName), collectorTableConfig);
        return this;
    }

    public CollectorConfig build() {
        CollectorConfig newConfig = config.copy();

        if (newConfig.sourceName == null) {
            newConfig.sourceName = getHostname();
        }

        return newConfig;
    }

    private static List<String> getAllTables(DataSource dataSource, String schema) {
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? and table_type = 'BASE TABLE'";

        return query(dataSource, sql, (statement) -> {
            statement.setString(1, schema);

            ResultSet resultSet = statement.executeQuery();

            return resultList(resultSet, (rs) -> rs.getString(1));
        });
    }

    private static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw softenException(e);
        }
    }

}
