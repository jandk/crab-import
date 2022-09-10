package be.twofold.crab.model;

import java.util.*;

public final class Table {

    private final String name;
    private final List<Column> columns;

    public Table(String name, List<Column> columns) {
        this.name = Objects.requireNonNull(name);
        this.columns = List.copyOf(columns);
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Table)) return false;

        Table table = (Table) obj;
        return name.equals(table.name)
            && columns.equals(table.columns);
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + name.hashCode();
        result = 31 * result + columns.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Table(" +
            "name=" + name + ", " +
            "columns=" + columns +
            ")";
    }

}
