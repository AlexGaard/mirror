package com.github.alexgaard.mirror.test_utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.alexgaard.mirror.postgres.utils.QueryUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;

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

}
