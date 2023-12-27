package com.github.alexgaard.mirror.common_test;

import com.github.alexgaard.mirror.core.utils.ExceptionUtil;

import java.time.Duration;
import java.time.LocalDateTime;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;

public class AsyncUtils {

    public static void eventually(ExceptionUtil.UnsafeRunnable runnable) {
        eventually(Duration.ofSeconds(10), Duration.ofMillis(50), runnable);
    }

    public static void eventually(Duration until, ExceptionUtil.UnsafeRunnable runnable) {
        eventually(until, Duration.ofMillis(50), runnable);
    }

    public static void eventually(Duration until, Duration interval, ExceptionUtil.UnsafeRunnable runnable) {
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

}

