package com.github.alexgaard.mirror.core;

import com.github.alexgaard.mirror.core.event.EventTransaction;

public interface EventSender {

    Result send(EventTransaction transaction);

}
