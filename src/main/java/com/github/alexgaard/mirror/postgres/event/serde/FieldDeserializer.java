package com.github.alexgaard.mirror.postgres.event.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.github.alexgaard.mirror.postgres.event.Field;

import java.io.IOException;

public class FieldDeserializer extends StdDeserializer<Field<?>> {

    public FieldDeserializer() {
        this(null);
    }

    protected FieldDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Field<?> deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException {
        final ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
        final JsonNode node = mapper.readTree(jsonParser);

        String name = node.get("name").asText();
        Field.Type type = Field.Type.valueOf(node.get("type").asText());
        JsonNode valueNode = node.get("value");

        return toField(name, type, valueNode);
    }

    private static Field<?> toField(String name, Field.Type type, JsonNode valueNode) {
        switch (type) {
            case FLOAT:
                return new Field.Float(name, (float) valueNode.asDouble());
            case INT32:
                return new Field.Int32(name, valueNode.asInt());
            case TEXT:
                return new Field.Text(name, valueNode.asText());
            default:
                throw new IllegalArgumentException("Missing deserialization implementation for field of type: " + type);
        }
    }

}
