package com.github.alexgaard.mirror.postgres.processor.config;


import static com.github.alexgaard.mirror.postgres.metadata.PgMetadata.tableFullName;

public class ProcessorConfigBuilder {

    private final static String DEFAULT_SCHEMA = "public";

    private final ProcessorConfig config = new ProcessorConfig();

    public ProcessorConfigBuilder configure(String tableName, ProcessorTableConfig tableConfig) {
        return configure(DEFAULT_SCHEMA, tableName, tableConfig);
    }

    public ProcessorConfigBuilder configure(String schema, String tableName, ProcessorTableConfig tableConfig) {
        config.tableConfig.put(tableFullName(schema, tableName), tableConfig);
        return this;
    }

    public ProcessorConfig build() {
        return config.copy();
    }

}
