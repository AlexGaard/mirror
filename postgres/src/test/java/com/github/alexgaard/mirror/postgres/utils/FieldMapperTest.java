package com.github.alexgaard.mirror.postgres.utils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.alexgaard.mirror.postgres.utils.FieldMapper.splitPostgresArray;
import static org.junit.jupiter.api.Assertions.*;

public class FieldMapperTest {

    @Test
    public void shouldSplitPostgresArray() {
        assertEquals(List.of("1","2","3"), splitPostgresArray("{1,2,3}"));
        assertEquals(List.of("helloworld","test"), splitPostgresArray("{helloworld,test}"));
        assertEquals(List.of("hello,world","test"), splitPostgresArray("{\"hello,world\",test}"));
        assertEquals(List.of("hello\"world"), splitPostgresArray("{\"hello\\\"world\"}"));
        assertEquals(List.of("hello\" world", "test"), splitPostgresArray("{\"hello\\\" world\",test}"));
        assertEquals(List.of("\"helloworld\""), splitPostgresArray("{\"\\\"helloworld\\\"\"}"));
    }

    @Test
    public void shouldThrowIfNotPostgresArray() {
        assertThrows(IllegalArgumentException.class, () -> splitPostgresArray("[42,56]"));
    }

}
