package com.github.alexgaard.mirror.postgres_serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.postgres.event.FieldType;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


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
        FieldType type = FieldType.valueOf(node.get("type").asText());
        JsonNode valueNode = node.get("value");

        return toField(name, type, valueNode);
    }

    private static Field<?> toField(String name, FieldType type, JsonNode valueNode) throws IOException {
        if (valueNode.isNull()) {
            return new Field<>(name, type, null);
        }

        switch (type) {
            case FLOAT:
                return Field.floatField(name, valueNode.floatValue());
            case DOUBLE:
                return Field.doubleField(name, valueNode.doubleValue());
            case BOOLEAN:
                return Field.booleanField(name, valueNode.booleanValue());
            case TEXT:
                return Field.textField(name, valueNode.textValue());
            case JSON:
                return Field.jsonField(name, valueNode.textValue());
            case JSONB:
                return Field.jsonbField(name, valueNode.textValue());
            case UUID:
                return Field.uuidField(name, UUID.fromString(valueNode.textValue()));
            case CHAR:
                return Field.charField(name, valueNode.textValue().charAt(0));
            case INT16:
                return Field.int16Field(name, valueNode.shortValue());
            case INT32:
                return Field.int32Field(name, valueNode.intValue());
            case INT64:
                return Field.int64Field(name, valueNode.longValue());
            case BYTES:
                return Field.bytesField(name, valueNode.binaryValue());
            case DATE:
                return Field.dateField(name, LocalDate.parse(valueNode.textValue()));
            case TIME:
                return Field.timeField(name, LocalTime.parse(valueNode.textValue()));
            case TIMESTAMP:
                return Field.timestampField(name, LocalDateTime.parse(valueNode.textValue()));
            case TIMESTAMP_TZ:
                return Field.timestampTzField(name, OffsetDateTime.parse(valueNode.textValue()));

            case FLOAT_ARRAY: {
                List<Float> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::floatValue)
                        .collect(Collectors.toList());

                return Field.floatArrayField(name, values);
            }
            case DOUBLE_ARRAY: {
                List<Double> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::doubleValue)
                        .collect(Collectors.toList());

                return Field.doubleArrayField(name, values);
            }
            case BOOLEAN_ARRAY: {
                List<Boolean> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::booleanValue)
                        .collect(Collectors.toList());

                return Field.booleanArrayField(name, values);
            }
            case TEXT_ARRAY: {
                List<String> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::asText)
                        .collect(Collectors.toList());

                return Field.textArrayField(name, values);
            }
            case UUID_ARRAY: {
                List<UUID> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::asText)
                        .map(UUID::fromString)
                        .collect(Collectors.toList());

                return Field.uuidArrayField(name, values);
            }
            case CHAR_ARRAY: {
                List<Character> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(n -> n.asText().charAt(0))
                        .collect(Collectors.toList());

                return Field.charArrayField(name, values);
            }
            case INT16_ARRAY: {
                List<Short> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::shortValue)
                        .collect(Collectors.toList());

                return Field.int16ArrayField(name, values);
            }
            case INT32_ARRAY: {
                List<Integer> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::intValue)
                        .collect(Collectors.toList());

                return Field.int32ArrayField(name, values);
            }
            case INT64_ARRAY: {
                List<Long> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::longValue)
                        .collect(Collectors.toList());

                return Field.int64ArrayField(name, values);
            }
            case DATE_ARRAY: {
                List<LocalDate> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::asText)
                        .map(LocalDate::parse)
                        .collect(Collectors.toList());

                return Field.dateArrayField(name, values);
            }
            case TIME_ARRAY: {
                List<LocalTime> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::asText)
                        .map(LocalTime::parse)
                        .collect(Collectors.toList());

                return Field.timeArrayField(name, values);
            }
            case TIMESTAMP_ARRAY: {
                List<LocalDateTime> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::asText)
                        .map(LocalDateTime::parse)
                        .collect(Collectors.toList());

                return Field.timestampArrayField(name, values);
            }
            case TIMESTAMP_TZ_ARRAY: {
                List<OffsetDateTime> values = StreamSupport.stream(valueNode.spliterator(), false)
                        .map(JsonNode::asText)
                        .map(OffsetDateTime::parse)
                        .collect(Collectors.toList());

                return Field.timestampTzArrayField(name, values);
            }
            default:
                throw new IllegalArgumentException("Missing deserialization implementation for field of type: " + type);
        }
    }

}
