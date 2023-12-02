package com.github.alexgaard.mirror.test_utils;

import org.testcontainers.containers.GenericContainer;

public class RabbitMqContainer {

    private static final String RABBIT_MQ_IMAGE = "rabbitmq:3.12-alpine";

    public static GenericContainer<?> rabbitMq = new GenericContainer<>(RABBIT_MQ_IMAGE)
            .withExposedPorts(5672, 8080)
            .withEnv("RABBITMQ_DEFAULT_USER", "user")
            .withEnv("RABBITMQ_DEFAULT_PASS", "qwerty");


}
