package com.github.alexgaard.mirror.core;

import com.github.alexgaard.mirror.core.event.EventTransactionConsumer;


public interface EventCollector {

    void setOnTransactionCollected(EventTransactionConsumer onTransactionCollected);

    void start();

    void stop();

}
