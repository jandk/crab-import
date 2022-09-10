package be.twofold.crab.model;

import java.sql.*;
import java.util.*;

public final class Column implements Named {
    private final String name;
    private final int sqlType;
    private final Integer length;
    private final boolean nullable;

    public Column(String name, int sqlType, Integer length, boolean nullable) {
        this.name = Objects.requireNonNull(name);
        this.sqlType = sqlType;
        this.length = length;
        this.nullable = nullable;
    }

    public static Column fixed(String name, int sqlType) {
        return new Column(name, sqlType, 0, false);
    }

    public static Column variable(String name, int sqlType, int length) {
        return new Column(name, sqlType, length, false);
    }

    public Column withNulls() {
        return new Column(name, sqlType, length, true);
    }

    @Override
    public String getName() {
        return name;
    }

    public int getSqlType() {
        return sqlType;
    }

    public Integer getLength() {
        return length;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String getTypeString() {
        String nullString = nullable ? " null" : " not null";
        switch (sqlType) {
            case Types.DATE:
                return "date" + nullString;
            case Types.FLOAT:
                return "real" + nullString;
            case Types.INTEGER:
                return "int" + nullString;
            case Types.SMALLINT:
                return "smallint" + nullString;
            case Types.TIMESTAMP:
                return "timestamp" + nullString;
            case Types.VARCHAR:
                return "varchar(" + length + ")" + nullString;
            default:
                throw new IllegalStateException("Unexpected type: " + sqlType);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Column)) return false;

        Column other = (Column) obj;
        return Objects.equals(name, other.name)
            && sqlType == other.sqlType
            && Objects.equals(length, other.length)
            && nullable == other.nullable;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + name.hashCode();
        result = 31 * result + Integer.hashCode(sqlType);
        result = 31 * result + Objects.hashCode(length);
        result = 31 * result + Boolean.hashCode(nullable);
        return result;
    }

    @Override
    public String toString() {
        return "Column(" +
            "name=" + name + ", " +
            "sqlType=" + sqlType + ", " +
            "length=" + length + ", " +
            "nullable=" + nullable +
            ")";
    }

}
