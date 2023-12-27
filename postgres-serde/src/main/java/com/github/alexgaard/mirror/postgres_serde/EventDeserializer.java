package com.github.alexgaard.mirror.postgres_serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.github.alexgaard.mirror.core.Event;
import com.github.alexgaard.mirror.postgres.event.*;

import java.io.IOException;

public class EventDeserializer extends StdDeserializer<Event> {

    public EventDeserializer() {
        this(null);
    }

    protected EventDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Event deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException {
        final ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
        final JsonNode node = mapper.readTree(jsonParser);

        String type = node.get("type").asText();

        if (PostgresTransactionEvent.TYPE.equals(type)) {
            return mapper.treeToValue(node, PostgresTransactionEvent.class);
        }

        return mapper.treeToValue(node, Event.class);
    }

}
