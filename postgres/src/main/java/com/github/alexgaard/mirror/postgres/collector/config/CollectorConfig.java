package com.github.alexgaard.mirror.postgres.collector.config;

import java.time.Duration;
import java.util.*;

public class CollectorConfig {

    // Key = "<schema>.<table_name>"
    final Map<String, TableConfig> tableConfig;

    final Map<String, Set<String>> schemaAndIncludedTables;

    String sourceName;

    String replicationSlotName;

    String publicationName;

    int maxChangesPrPoll = 500;

    Duration pollInterval = Duration.ofSeconds(1);

    Duration backoffIncrease = Duration.ofSeconds(1);

    Duration maxBackoff = Duration.ofSeconds(10);

    public CollectorConfig(
            Map<String, TableConfig> tableConfig,
            Map<String, Set<String>> schemaAndIncludedTables,
            String sourceName,
            String replicationSlotName,
            String publicationName,
            int maxChangesPrPoll,
            Duration pollInterval,
            Duration backoffIncrease,
            Duration maxBackoff
    ) {
        this.tableConfig = tableConfig;
        this.schemaAndIncludedTables = schemaAndIncludedTables;
        this.sourceName = sourceName;
        this.replicationSlotName = replicationSlotName;
        this.publicationName = publicationName;
        this.maxChangesPrPoll = maxChangesPrPoll;
        this.pollInterval = pollInterval;
        this.backoffIncrease = backoffIncrease;
        this.maxBackoff = maxBackoff;
    }

    public CollectorConfig(String sourceName, String replicationSlotName, String publicationName) {
        this.tableConfig = new HashMap<>();
        this.schemaAndIncludedTables = new HashMap<>();
        this.sourceName = sourceName;
        this.replicationSlotName = replicationSlotName;
        this.publicationName = publicationName;
    }

    public Map<String, TableConfig> getTableConfig() {
        return tableConfig;
    }

    public Map<String, Set<String>> getSchemaAndIncludedTables() {
        return schemaAndIncludedTables;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getReplicationSlotName() {
        return replicationSlotName;
    }

    public String getPublicationName() {
        return publicationName;
    }

    public int getMaxChangesPrPoll() {
        return maxChangesPrPoll;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public Duration getBackoffIncrease() {
        return backoffIncrease;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public CollectorConfig copy() {
        Map<String, TableConfig> tableConfigCopy = new HashMap<>();
        tableConfig.forEach((k, v) -> tableConfig.put(k, v.copy()));

        Map<String, Set<String>> schemaAndIncludedTablesCopy = new HashMap<>();
        schemaAndIncludedTables.forEach((k, v) -> schemaAndIncludedTablesCopy.put(k, new HashSet<>(v)));

        return new CollectorConfig(
                tableConfigCopy,
                schemaAndIncludedTablesCopy,
                sourceName,
                replicationSlotName,
                publicationName,
                maxChangesPrPoll,
                pollInterval,
                backoffIncrease,
                maxBackoff
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CollectorConfig that = (CollectorConfig) o;

        if (maxChangesPrPoll != that.maxChangesPrPoll) return false;
        if (!tableConfig.equals(that.tableConfig)) return false;
        if (!schemaAndIncludedTables.equals(that.schemaAndIncludedTables)) return false;
        if (!Objects.equals(sourceName, that.sourceName)) return false;
        if (!replicationSlotName.equals(that.replicationSlotName)) return false;
        return publicationName.equals(that.publicationName);
    }

    @Override
    public int hashCode() {
        int result = tableConfig.hashCode();
        result = 31 * result + schemaAndIncludedTables.hashCode();
        result = 31 * result + (sourceName != null ? sourceName.hashCode() : 0);
        result = 31 * result + replicationSlotName.hashCode();
        result = 31 * result + publicationName.hashCode();
        result = 31 * result + maxChangesPrPoll;
        return result;
    }

    @Override
    public String toString() {
        return "CollectorConfig{" +
                "tableConfig=" + tableConfig +
                ", schemaAndIncludedTables=" + schemaAndIncludedTables +
                ", sourceName='" + sourceName + '\'' +
                ", replicationSlotName='" + replicationSlotName + '\'' +
                ", publicationName='" + publicationName + '\'' +
                ", maxChangesPrPoll=" + maxChangesPrPoll +
                '}';
    }
}
