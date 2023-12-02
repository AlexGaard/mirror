package com.github.alexgaard.mirror.core;

import com.github.alexgaard.mirror.core.event.EventTransactionConsumer;

public interface EventReceiver {

    void initialize(EventTransactionConsumer onEventReceived);

    void start();

    void stop();

}
