package com.github.alexgaard.mirror.postgres_serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.alexgaard.mirror.core.Event;
import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.core.serde.Deserializer;
import com.github.alexgaard.mirror.core.serde.Serializer;
import com.github.alexgaard.mirror.postgres.event.PostgresEvent;

public class JsonSerde {

    public static final ObjectMapper jsonMapper = createMapper();

    public static final Serializer jsonSerializer = jsonMapper::writeValueAsString;

    public static final Deserializer jsonDeserializer = (data -> jsonMapper.readValue(data, Event.class));

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        SimpleModule deserializers = new SimpleModule();
        deserializers.addDeserializer(Event.class, new EventDeserializer());
        deserializers.addDeserializer(PostgresEvent.class, new PostgresEventDeserializer());
        deserializers.addDeserializer(Field.class, new FieldDeserializer());
        mapper.registerModule(deserializers);

        return mapper;
    }

}
