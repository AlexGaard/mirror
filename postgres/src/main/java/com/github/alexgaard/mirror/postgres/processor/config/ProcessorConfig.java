package com.github.alexgaard.mirror.postgres.processor.config;


import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class ProcessorConfig {

    final Map<String, ProcessorTableConfig> tableConfig;

    CustomMessageHandler customMessageHandler;

    public ProcessorConfig() {
        this.tableConfig = new HashMap<>();
    }

    public ProcessorConfig(Map<String, ProcessorTableConfig> tableConfig, CustomMessageHandler customMessageHandler) {
        this.tableConfig = tableConfig;
        this.customMessageHandler = customMessageHandler;
    }

    public Map<String, ProcessorTableConfig> getTableConfig() {
        return tableConfig;
    }

    public CustomMessageHandler getCustomMessageHandler() {
        return customMessageHandler;
    }

    public ProcessorConfig copy() {
        Map<String, ProcessorTableConfig> tableConfigCopy = new HashMap<>();
        tableConfig.forEach((k, v) -> tableConfigCopy.put(k, v.copy()));

        return new ProcessorConfig(tableConfigCopy, customMessageHandler);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProcessorConfig config = (ProcessorConfig) o;

        if (!Objects.equals(tableConfig, config.tableConfig)) return false;
        return Objects.equals(customMessageHandler, config.customMessageHandler);
    }

    @Override
    public int hashCode() {
        int result = tableConfig != null ? tableConfig.hashCode() : 0;
        result = 31 * result + (customMessageHandler != null ? customMessageHandler.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ProcessorConfig{" +
                "tableConfig=" + tableConfig +
                ", customMessageHandler=" + customMessageHandler +
                '}';
    }
}
