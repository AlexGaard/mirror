package com.github.alexgaard.mirror.postgres.event.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.postgres.utils.DateParser;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;


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

    private static Field<?> toField(String name, Field.Type type, JsonNode valueNode) throws IOException {
        switch (type) {
            case FLOAT:
                return new Field.Float(name, valueNode.floatValue());
            case DOUBLE:
                return new Field.Double(name, valueNode.doubleValue());
            case BOOLEAN:
                return new Field.Boolean(name, valueNode.booleanValue());
            case TEXT:
                return new Field.Text(name, valueNode.textValue());
            case JSON:
                return new Field.Json(name, valueNode.textValue());
            case UUID:
                return new Field.Uuid(name, UUID.fromString(valueNode.textValue()));
            case CHAR:
                return new Field.Char(name, valueNode.textValue().charAt(0));
            case INT16:
                return new Field.Int16(name, valueNode.shortValue());
            case INT32:
                return new Field.Int32(name, valueNode.intValue());
            case INT64:
                return new Field.Int64(name, valueNode.longValue());
            case BYTES:
                // TODO: verify
                return new Field.Bytes(name, valueNode.binaryValue());
            case DATE:
                return new Field.Date(name, LocalDate.parse(valueNode.textValue()));
            case TIME:
                return new Field.Time(name, LocalTime.parse(valueNode.textValue()));
            case TIMESTAMP:
                return new Field.Timestamp(name, LocalDateTime.parse(valueNode.textValue()));
            case TIMESTAMP_TZ:
                return new Field.TimestampTz(name, OffsetDateTime.parse(valueNode.textValue()));
            case NULL:
                return new Field.Null(name);
            default:
                throw new IllegalArgumentException("Missing deserialization implementation for field of type: " + type);
        }
    }

}
