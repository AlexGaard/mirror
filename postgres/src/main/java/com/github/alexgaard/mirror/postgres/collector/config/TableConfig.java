package com.github.alexgaard.mirror.postgres.collector.config;

public class TableConfig {

    // The name of the preferred constraint to use if multiple constraints
    // are available or if only constraints with one or more nullable fields are available
    public final String preferredConstraint;

    public TableConfig(String preferredConstraint) {
        this.preferredConstraint = preferredConstraint;
    }

    public TableConfig copy() {
        return new TableConfig(preferredConstraint);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableConfig that = (TableConfig) o;

        return preferredConstraint.equals(that.preferredConstraint);
    }

    @Override
    public int hashCode() {
        return preferredConstraint.hashCode();
    }

    @Override
    public String toString() {
        return "TableConfig{" +
                "preferredConstraint='" + preferredConstraint + '\'' +
                '}';
    }
}
