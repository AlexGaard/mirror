package com.github.alexgaard.mirror.postgres.event.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.github.alexgaard.mirror.postgres.event.Field;

import java.io.IOException;

public class FieldDeserializer extends StdDeserializer<Field> {

    public FieldDeserializer() {
        this(null);
    }

    protected FieldDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Field deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException {
        final ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
        final JsonNode node = mapper.readTree(jsonParser);

        Field.Type type = Field.Type.valueOf(node.get("type").asText());
        JsonNode valueNode = node.get("value");

        Object mappedValue = mapValue(type, valueNode);

        return new Field(node.get("name").asText(), type, mappedValue);
    }

    private static Object mapValue(Field.Type type, JsonNode valueNode) {
        switch (type) {
            case FLOAT:
                return valueNode.asDouble();
            case INT32:
                return valueNode.asInt();
            case STRING:
                return valueNode.asText();
            default:
                throw new IllegalArgumentException("Missing deserialization implementation for field of type: " + type);
        }
    }

}
