package com.github.alexgaard.mirror.common_test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class PostgresSingletonContainer {

    private static PostgresContainerWrapper container;

    public static synchronized DataSource getDataSource() {
//        HikariConfig dataSource = new HikariConfig();
//        dataSource.setUsername("postgres");
//        dataSource.setPassword("qwerty");
//        dataSource.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
//
//        return new HikariDataSource(dataSource);
        return getContainer().getDataSource();
    }

    private static synchronized PostgresContainerWrapper getContainer() {
        if (container == null) {
            container = new PostgresContainerWrapper();
            container.start();
        }

        return container;
    }


}
