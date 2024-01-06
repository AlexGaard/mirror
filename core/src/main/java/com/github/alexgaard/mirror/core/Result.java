package com.github.alexgaard.mirror.core;

import java.util.Objects;
import java.util.Optional;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;

public abstract class Result {

    private Result() {
    }

    public static Result ok() {
        return Ok.INSTANCE;
    }

    public static Result error(Exception exception) {
        return new Error(exception);
    }

    public abstract boolean isOk();

    public abstract boolean isError();

    public abstract Optional<Exception> getError();

    public abstract void throwIfError();

    public static class Ok extends Result {
        final static Ok INSTANCE = new Ok();

        private Ok() {
        }

        @Override
        public String toString() {
            return "Ok{}";
        }

        @Override
        public boolean isOk() {
            return true;
        }

        @Override
        public boolean isError() {
            return false;
        }

        @Override
        public Optional<Exception> getError() {
            return Optional.empty();
        }

        @Override
        public void throwIfError() {
            // NOOP
        }
    }

    public static class Error extends Result {

        public final Exception exception;

        private Error(Exception exception) {
            this.exception = Objects.requireNonNullElseGet(exception, () -> new RuntimeException("Status: error"));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Error error = (Error) o;
            return Objects.equals(exception, error.exception);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exception);
        }

        @Override
        public String toString() {
            return "Error{" +
                    "exception=" + exception +
                    '}';
        }

        @Override
        public boolean isOk() {
            return false;
        }

        @Override
        public boolean isError() {
            return true;
        }

        @Override
        public Optional<Exception> getError() {
            return Optional.of(exception);
        }

        @Override
        public void throwIfError() {
           throw softenException(exception);
        }
    }

}
