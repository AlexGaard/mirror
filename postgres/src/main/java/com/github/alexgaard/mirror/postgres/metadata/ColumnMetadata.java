package com.github.alexgaard.mirror.postgres.metadata;

public class ColumnMetadata {

    public final String schemaName;

    public final String tableName;


    public final String name;

    public final int ordinalPosition;

    public final boolean isNullable;

    public ColumnMetadata(String schemaName, String tableName, String name, int ordinalPosition, boolean isNullable) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.name = name;
        this.ordinalPosition = ordinalPosition;
        this.isNullable = isNullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnMetadata that = (ColumnMetadata) o;

        if (ordinalPosition != that.ordinalPosition) return false;
        if (isNullable != that.isNullable) return false;
        if (!schemaName.equals(that.schemaName)) return false;
        if (!tableName.equals(that.tableName)) return false;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = schemaName.hashCode();
        result = 31 * result + tableName.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + ordinalPosition;
        result = 31 * result + (isNullable ? 1 : 0);
        return result;
    }
}
