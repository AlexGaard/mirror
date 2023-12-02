package com.github.alexgaard.mirror.core.event;

public interface EventTransactionConsumer {

    void consume(EventTransaction transaction);

}
