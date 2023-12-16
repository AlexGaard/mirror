package com.github.alexgaard.mirror.test_utils;

import java.time.Duration;
import java.time.LocalDateTime;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;
import static java.lang.String.format;

public class AsyncUtils {

    public static void eventually(UnsafeRunnable runnable) {
        eventually(Duration.ofSeconds(10), Duration.ofMillis(50), runnable);
    }

    public static void eventually(Duration until, UnsafeRunnable runnable) {
        eventually(until, Duration.ofMillis(50), runnable);
    }

    public static void eventually(Duration until, Duration interval, UnsafeRunnable runnable) {
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

        throw softenException(throwable);
    }

    public interface UnsafeRunnable {
        void run() throws Exception;
    }

}

