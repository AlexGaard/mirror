package com.github.alexgaard.mirror.core;

import com.github.alexgaard.mirror.core.event.Event;
import com.github.alexgaard.mirror.core.event.EventTransaction;

import java.util.List;

public interface EventProcessor {

    void process(EventTransaction transaction);

}
