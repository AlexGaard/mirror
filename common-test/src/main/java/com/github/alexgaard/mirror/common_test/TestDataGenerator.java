package com.github.alexgaard.mirror.common_test;

import java.util.concurrent.atomic.AtomicInteger;

public class TestDataGenerator {

    private final static AtomicInteger id = new AtomicInteger();

    private final static AtomicInteger replicationNr = new AtomicInteger();

    private final static AtomicInteger rabbitMqExchangeNr = new AtomicInteger();
    private final static AtomicInteger rabbitMqQueueNr = new AtomicInteger();


    public static int newId() {
        return id.getAndIncrement();
    }

    public static String newReplicationName() {
        return "mirror_" + replicationNr.getAndIncrement();
    }

    public static String newRabbitMqExchange() {
        return "mirror_exchange_" + rabbitMqExchangeNr.getAndIncrement();
    }

    public static String newRabbitMqQueue() {
        return "mirror_queue_" + rabbitMqQueueNr.getAndIncrement();
    }

}
