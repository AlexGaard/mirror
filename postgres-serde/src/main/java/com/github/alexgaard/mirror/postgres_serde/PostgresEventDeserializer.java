package com.github.alexgaard.mirror.postgres_serde;

import com.github.alexgaard.mirror.postgres.event.*;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

public class PostgresEventDeserializer extends StdDeserializer<PostgresEvent> {

    public PostgresEventDeserializer() {
        this(null);
    }

    protected PostgresEventDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public PostgresEvent deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException {
        final ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
        final JsonNode node = mapper.readTree(jsonParser);

        String type = node.get("type").asText();

        switch (type) {
            case InsertEvent.TYPE: {
                return mapper.treeToValue(node, InsertEvent.class);
            }
            case UpdateEvent.TYPE: {
                return mapper.treeToValue(node, UpdateEvent.class);
            }
            case DeleteEvent.TYPE: {
                return mapper.treeToValue(node, DeleteEvent.class);
            }
            case CustomMessageEvent.TYPE: {
                return mapper.treeToValue(node, CustomMessageEvent.class);
            }
            default:
                throw new IllegalArgumentException("Unable to deserialize unknown event of type " + type);
        }
    }

}
