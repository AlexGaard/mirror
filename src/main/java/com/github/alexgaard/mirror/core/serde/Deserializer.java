package com.github.alexgaard.mirror.core.serde;

import com.github.alexgaard.mirror.core.event.EventTransaction;

import java.io.IOException;

public interface Deserializer {

    EventTransaction deserialize(String data) throws IOException;

}
