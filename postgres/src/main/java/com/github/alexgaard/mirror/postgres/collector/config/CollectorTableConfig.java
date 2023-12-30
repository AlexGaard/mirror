package com.github.alexgaard.mirror.postgres.collector.config;

public class CollectorTableConfig {

    // The name of the preferred constraint to use if multiple constraints
    // are available or if only constraints with one or more nullable fields are available
    public final String preferredConstraint;

    public CollectorTableConfig(String preferredConstraint) {
        this.preferredConstraint = preferredConstraint;
    }

    public CollectorTableConfig copy() {
        return new CollectorTableConfig(preferredConstraint);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CollectorTableConfig that = (CollectorTableConfig) o;

        return preferredConstraint.equals(that.preferredConstraint);
    }

    @Override
    public int hashCode() {
        return preferredConstraint.hashCode();
    }

    @Override
    public String toString() {
        return "CollectorTableConfig{" +
                "preferredConstraint='" + preferredConstraint + '\'' +
                '}';
    }
}
