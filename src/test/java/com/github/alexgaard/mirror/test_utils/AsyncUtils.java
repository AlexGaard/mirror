package com.github.alexgaard.mirror.test_utils;

import java.time.Duration;
import java.time.LocalDateTime;

import static java.lang.String.format;

public class AsyncUtils {

    public static void eventually(Runnable runnable) {
        eventually(Duration.ofSeconds(10), Duration.ofMillis(50), runnable);
    }

    public static void eventually(Duration until, Runnable runnable) {
        eventually(until, Duration.ofMillis(50), runnable);
    }

    public static void eventually(Duration until, Duration interval, Runnable runnable) {
        LocalDateTime untilTime = LocalDateTime.now().plusNanos(until.toNanos());

        Throwable throwable = null;

        while (LocalDateTime.now().isBefore(untilTime)) {
            try {
                runnable.run();
                return;
            } catch (Throwable t) {
                throwable = t;

                try {
                    Thread.sleep(interval.toMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        throw new AssertionError(format("Expected to complete within %s", until), throwable);
    }

}

