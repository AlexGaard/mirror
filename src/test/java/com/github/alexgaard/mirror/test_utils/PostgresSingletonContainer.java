package com.github.alexgaard.mirror.test_utils;

import com.rabbitmq.client.ConnectionFactory;

import javax.sql.DataSource;

public class PostgresSingletonContainer {

    private static PostgresContainerWrapper container;

    public static synchronized DataSource getDataSource() {
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
