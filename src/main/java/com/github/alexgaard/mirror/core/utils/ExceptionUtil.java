package com.github.alexgaard.mirror.core.utils;

import com.github.alexgaard.mirror.core.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class ExceptionUtil {

    private final static Logger log = LoggerFactory.getLogger(ExceptionUtil.class);

    /**
     * Uses template type erasure to trick the compiler into removing checking of exception. The compiler
     * treats E as RuntimeException, meaning that softenException doesn't need to declare it,
     * but the runtime treats E as Exception (because of type erasure), which avoids
     * {@link ClassCastException}.
     */
    public static <T extends Throwable> T softenException(Throwable t) throws T {
        if (t == null) throw new RuntimeException();
        //noinspection unchecked
        throw (T) t;
    }

    public static Runnable safeRunnable(Runnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (Exception e) {
                log.error("Caught exception from runnable", e);
            }
        };
    }

    public static Result runWithResult(Supplier<Result> unsafeSupplier) {
        try {
            return unsafeSupplier.get();
        } catch (Exception e) {
            return Result.error(e);
        }
    }

}
