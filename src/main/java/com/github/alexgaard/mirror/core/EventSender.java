package com.github.alexgaard.mirror.core;

import com.github.alexgaard.mirror.core.event.EventTransaction;

public interface EventSender {

    void send(EventTransaction transaction);

}
