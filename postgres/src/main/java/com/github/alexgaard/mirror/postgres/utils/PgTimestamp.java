package com.github.alexgaard.mirror.postgres.utils;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class PgTimestamp {

    // pgTimestamp is in number of microseconds since PostgreSQL epoch (2000-01-01).
    public static OffsetDateTime toOffsetDateTime(long pgTimestamp) {
        long timestampMs = pgTimestamp / 1000;
        long micros = pgTimestamp % 1000;

        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestampMs), ZoneOffset.UTC)
                .plusYears(30)
                .plusNanos(micros * 1000);
    }

}
