package com.github.alexgaard.mirror.common_test;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitMqSingletonContainer {

    private static RabbitMQContainerWrapper container;

    public static synchronized ConnectionFactory createConnectionFactory() {
        return getContainer().createConnectionFactory();
    }

    public static synchronized void setupExchangeWithQueue(String queue, String exchange, String routingKey) {
        getContainer().setupExchangeWithQueue(queue, exchange, routingKey);
    }

    private static synchronized RabbitMQContainerWrapper getContainer() {
        if (container == null) {
            container = new RabbitMQContainerWrapper();
            container.start();
        }

        return container;
    }

}
