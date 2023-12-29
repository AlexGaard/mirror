package com.github.alexgaard.mirror.postgres.collector;

public class TableReplicationConfig {

    // The name of the preferred constraint to use if multiple constraints
    // are available or if only constraints with one or more nullable fields are available
    public final String preferredConstraint;

    public TableReplicationConfig(String preferredConstraint) {
        this.preferredConstraint = preferredConstraint;
    }

}
