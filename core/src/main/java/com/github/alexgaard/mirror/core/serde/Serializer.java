package com.github.alexgaard.mirror.core.serde;

import com.github.alexgaard.mirror.core.EventTransaction;

import java.io.IOException;

public interface Serializer {

    String serialize(EventTransaction transaction) throws IOException;

}
