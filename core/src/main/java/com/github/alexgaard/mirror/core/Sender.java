package com.github.alexgaard.mirror.core;

import com.github.alexgaard.mirror.core.event.EventTransaction;

public interface Sender {

    Result send(EventTransaction transaction);

}
