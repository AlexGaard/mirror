package com.github.alexgaard.mirror.postgres.processor.config;


import java.util.HashMap;
import java.util.Map;

public class ProcessorConfig {

    final Map<String, ProcessorTableConfig> tableConfig;

    public ProcessorConfig() {
        this.tableConfig = new HashMap<>();
    }

    public ProcessorConfig(Map<String, ProcessorTableConfig> tableConfig) {
        this.tableConfig = tableConfig;
    }

    public Map<String, ProcessorTableConfig> getTableConfig() {
        return tableConfig;
    }

    public ProcessorConfig copy() {
        Map<String, ProcessorTableConfig> tableConfigCopy = new HashMap<>();
        tableConfig.forEach((k, v) -> tableConfigCopy.put(k, v.copy()));

        return new ProcessorConfig(tableConfigCopy);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProcessorConfig config = (ProcessorConfig) o;

        return tableConfig.equals(config.tableConfig);
    }

    @Override
    public int hashCode() {
        return tableConfig.hashCode();
    }

    @Override
    public String toString() {
        return "ProcessorConfig{" +
                "tableConfig=" + tableConfig +
                '}';
    }
}
