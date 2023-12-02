package com.github.alexgaard.mirror.test_utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

public class PostgresContainer {

    private final PostgreSQLContainer<?> container;

    private DataSource dataSource;

    public PostgresContainer() {
        container = new PostgreSQLContainer<>("16.0-alpine3.18");
    }

    public void start() {
        container.start();
        dataSource = createDataSource();
    }

    public void stop() {
        container.stop();
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    private DataSource createDataSource() {
        var config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(1);
        config.setUsername(container.getUsername());
        config.setPassword(config.getPassword());

        return new HikariDataSource(config);
    }

}
