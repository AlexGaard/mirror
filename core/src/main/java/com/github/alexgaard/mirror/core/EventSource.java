package com.github.alexgaard.mirror.core;


public interface EventSource {

    void setEventSink(EventSink eventSink);

    void start();

    void stop();

}
