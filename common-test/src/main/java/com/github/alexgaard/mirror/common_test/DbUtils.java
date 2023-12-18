package com.github.alexgaard.mirror.common_test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;

import static com.github.alexgaard.mirror.common_test.QueryUtils.query;
import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;

public class DbUtils {

    public static DataSource createDataSource(PostgreSQLContainer<?> container) {
        var config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(1);
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());

        return new HikariDataSource(config);
    }

    public static void initTables(DataSource dataSource) {
        executeSqlResourceScript(dataSource, "tables.sql");
    }

    public static void executeSqlResourceScript(DataSource dataSource, String resourceName) {
       try {
           URL resource = DbUtils.class.getClassLoader().getResource(resourceName);
           String initSql = Files.readString(new File(resource.toURI()).toPath());

           QueryUtils.update(dataSource, initSql);
       } catch (Exception e) {
           throw softenException(e);
       }
    }

    public static void drainWalMessages(DataSource dataSource, String replicationSlotName, String publicationName) {
        String sql = "SELECT 1 FROM pg_logical_slot_get_binary_changes(?, NULL, ?, 'messages', 'true', 'proto_version', '1', 'publication_names', ?)";

        query(dataSource, sql, statement -> {
            statement.setString(1, replicationSlotName);
            statement.setInt(2, 10000);
            statement.setString(3, publicationName);
            statement.executeQuery();
        });
    }

}
