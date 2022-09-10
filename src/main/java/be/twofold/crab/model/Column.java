package be.twofold.crab.model;

import java.sql.*;
import java.util.*;

public record Column(String name, int sqlType, Integer length, boolean nullable) implements Named {
    public Column {
        Objects.requireNonNull(name);
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

    public String getTypeString() {
        return switch (sqlType) {
            case Types.DATE -> "date";
            case Types.FLOAT -> "real";
            case Types.INTEGER -> "int";
            case Types.SMALLINT -> "smallint";
            case Types.VARCHAR -> "varchar(" + length + ")";
            default -> throw new IllegalStateException("Unexpected type: " + sqlType);
        } + (nullable ? " null" : " not null");
    }
}
