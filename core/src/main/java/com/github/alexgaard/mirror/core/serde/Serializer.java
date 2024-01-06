package com.github.alexgaard.mirror.core.serde;

import com.github.alexgaard.mirror.core.Event;

import java.io.IOException;

public interface Serializer {

    byte[] serialize(Event transaction) throws IOException;

}
