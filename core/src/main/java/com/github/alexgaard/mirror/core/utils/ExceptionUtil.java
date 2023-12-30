package com.github.alexgaard.mirror.core.utils;

import com.github.alexgaard.mirror.core.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class ExceptionUtil {

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

    public static Result runWithResult(UnsafeSupplier<Result> unsafeSupplier) {
        try {
            Result result = unsafeSupplier.get();

            if (result == null) {
                return Result.error(new IllegalStateException("Result was null"));
            }

            return result;
        } catch (Exception e) {
            return Result.error(e);
        }
    }

    public interface UnsafeRunnable {
        void run() throws Exception;

    }

    public interface UnsafeSupplier<T> {
        T get() throws Exception;

    }

}
