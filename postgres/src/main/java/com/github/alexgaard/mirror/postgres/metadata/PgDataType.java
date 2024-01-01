package com.github.alexgaard.mirror.postgres.metadata;

import com.github.alexgaard.mirror.postgres.event.FieldType;

public class PgDataType {
    public final String typeName;

    public final FieldType fieldType;

    public PgDataType(String pgType) {
        this.typeName = pgType;
        this.fieldType = FieldType.ofPgType(pgType);
    }

    public FieldType getType() {
       return this.fieldType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PgDataType that = (PgDataType) o;

        if (!typeName.equals(that.typeName)) return false;
        return fieldType == that.fieldType;
    }

    @Override
    public int hashCode() {
        int result = typeName.hashCode();
        result = 31 * result + fieldType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PgDataType{" +
                "typeName='" + typeName + '\'' +
                ", fieldType=" + fieldType +
                '}';
    }
}