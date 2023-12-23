package com.github.alexgaard.mirror.core;


public interface EventSink {

    Result consume(Event event);

}
