package com.github.alexgaard.mirror.core;

import com.github.alexgaard.mirror.core.event.EventTransactionConsumer;


public interface EventCollector {

    void initialize(EventTransactionConsumer onTransactionCollected);

    void start();

    void stop();

}
