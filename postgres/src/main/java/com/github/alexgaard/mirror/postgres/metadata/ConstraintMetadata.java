package com.github.alexgaard.mirror.postgres.metadata;

import java.util.List;

public class ConstraintMetadata {
    public enum ConstraintType {
        PRIMARY_KEY,
        UNIQUE,
        OTHER
    }

    public final String schemaName;
    public final String tableName;
    public final String constraintName;

    public final ConstraintType type;
    public final List<Integer> constraintKeyOrdinalPositions;

    public ConstraintMetadata(String schemaName, String tableName, String constraintName, ConstraintType type, List<Integer> constraintKeyOrdinalPositions) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.constraintName = constraintName;
        this.type = type;
        this.constraintKeyOrdinalPositions = constraintKeyOrdinalPositions;
    }

}
