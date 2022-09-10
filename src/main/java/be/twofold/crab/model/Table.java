package be.twofold.crab.model;

import java.util.*;

public record Table(String name, List<Column> columns) implements Named {
    public Table(String name, List<Column> columns) {
        this.name = Objects.requireNonNull(name);
        this.columns = List.copyOf(columns);
    }
}
