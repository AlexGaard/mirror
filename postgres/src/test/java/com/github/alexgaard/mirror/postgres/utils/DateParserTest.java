package com.github.alexgaard.mirror.postgres.utils;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DateParserTest {

    @Test
    public void should_parse_local_dates_with_variable_precision() {
        assertEquals(
                LocalDateTime.of(2023, 12, 18, 11, 0, 37, 0),
                DateParser.parseVariablePrecisionLocalDateTime("2023-12-18 11:00:37")
        );

        assertEquals(
                LocalDateTime.of(2023, 12, 18, 11, 0, 37, 600000000),
                DateParser.parseVariablePrecisionLocalDateTime("2023-12-18 11:00:37.6")
        );

        assertEquals(
                LocalDateTime.of(2023, 12, 18, 11, 0, 37, 680000000),
                DateParser.parseVariablePrecisionLocalDateTime("2023-12-18 11:00:37.68")
        );

        assertEquals(
                LocalDateTime.of(2023, 12, 18, 11, 0, 37, 683000000),
                DateParser.parseVariablePrecisionLocalDateTime("2023-12-18 11:00:37.683")
        );

        assertEquals(
                LocalDateTime.of(2023, 12, 18, 11, 0, 37, 683100000),
                DateParser.parseVariablePrecisionLocalDateTime("2023-12-18 11:00:37.6831")
        );

        assertEquals(
                LocalDateTime.of(2023, 12, 18, 11, 0, 37, 683140000),
                DateParser.parseVariablePrecisionLocalDateTime("2023-12-18 11:00:37.68314")
        );


        assertEquals(
                LocalDateTime.of(2023, 12, 18, 11, 0, 37, 683147000),
                DateParser.parseVariablePrecisionLocalDateTime("2023-12-18 11:00:37.683147")
        );

        assertEquals(
                LocalDateTime.of(2023, 12, 18, 11, 0, 37, 683147800),
                DateParser.parseVariablePrecisionLocalDateTime("2023-12-18 11:00:37.6831478")
        );

        assertEquals(
                LocalDateTime.of(2023, 12, 18, 11, 0, 37, 683147820),
                DateParser.parseVariablePrecisionLocalDateTime("2023-12-18 11:00:37.68314782")
        );

        assertEquals(
                LocalDateTime.of(2023, 12, 18, 11, 0, 37, 683147823),
                DateParser.parseVariablePrecisionLocalDateTime("2023-12-18 11:00:37.683147823")
        );
    }

    @Test
    public void should_parse_offset_dates_with_variable_precision() {
        assertEquals(
                OffsetDateTime.of(2023, 12, 18, 11, 0, 37, 0, ZoneOffset.UTC),
                DateParser.parseVariablePrecisionOffsetDateTime("2023-12-18 11:00:37Z")
        );

        assertEquals(
                OffsetDateTime.of(2023, 12, 18, 11, 0, 37, 600000000, ZoneOffset.UTC),
                DateParser.parseVariablePrecisionOffsetDateTime("2023-12-18 11:00:37.6Z")
        );

        assertEquals(
                OffsetDateTime.of(2023, 12, 18, 11, 0, 37, 680000000, ZoneOffset.UTC),
                DateParser.parseVariablePrecisionOffsetDateTime("2023-12-18 11:00:37.68Z")
        );

        assertEquals(
                OffsetDateTime.of(2023, 12, 18, 11, 0, 37, 683000000, ZoneOffset.UTC),
                DateParser.parseVariablePrecisionOffsetDateTime("2023-12-18 11:00:37.683Z")
        );

        assertEquals(
                OffsetDateTime.of(2023, 12, 18, 11, 0, 37, 683100000, ZoneOffset.UTC),
                DateParser.parseVariablePrecisionOffsetDateTime("2023-12-18 11:00:37.6831Z")
        );

        assertEquals(
                OffsetDateTime.of(2023, 12, 18, 11, 0, 37, 683150000, ZoneOffset.UTC),
                DateParser.parseVariablePrecisionOffsetDateTime("2023-12-18 11:00:37.68315Z")
        );

        assertEquals(
                OffsetDateTime.of(2023, 12, 18, 11, 0, 37, 683157000, ZoneOffset.UTC),
                DateParser.parseVariablePrecisionOffsetDateTime("2023-12-18 11:00:37.683157Z")
        );

        assertEquals(
                OffsetDateTime.of(2023, 12, 18, 11, 0, 37, 683157200, ZoneOffset.UTC),
                DateParser.parseVariablePrecisionOffsetDateTime("2023-12-18 11:00:37.6831572Z")
        );

        assertEquals(
                OffsetDateTime.of(2023, 12, 18, 11, 0, 37, 683157260, ZoneOffset.UTC),
                DateParser.parseVariablePrecisionOffsetDateTime("2023-12-18 11:00:37.68315726Z")
        );

        assertEquals(
                OffsetDateTime.of(2023, 12, 18, 11, 0, 37, 683157269, ZoneOffset.UTC),
                DateParser.parseVariablePrecisionOffsetDateTime("2023-12-18 11:00:37.683157269Z")
        );
    }

}
