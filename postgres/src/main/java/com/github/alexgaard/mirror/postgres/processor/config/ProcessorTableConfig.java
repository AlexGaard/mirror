package com.github.alexgaard.mirror.postgres.processor.config;


public class ProcessorTableConfig {

    public final String insertConflictConstraint;

    public final InsertConflictStrategy insertConflictStrategy;

    public ProcessorTableConfig(String insertConflictConstraint, InsertConflictStrategy insertConflictStrategy) {
        this.insertConflictConstraint = insertConflictConstraint;
        this.insertConflictStrategy = insertConflictStrategy;
    }

    public ProcessorTableConfig copy() {
        return new ProcessorTableConfig(insertConflictConstraint, insertConflictStrategy);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProcessorTableConfig that = (ProcessorTableConfig) o;

        if (!insertConflictConstraint.equals(that.insertConflictConstraint)) return false;
        return insertConflictStrategy == that.insertConflictStrategy;
    }

    @Override
    public int hashCode() {
        int result = insertConflictConstraint.hashCode();
        result = 31 * result + insertConflictStrategy.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ProcessorTableConfig{" +
                "insertConflictConstraint='" + insertConflictConstraint + '\'' +
                ", insertConflictStrategy=" + insertConflictStrategy +
                '}';
    }
}
