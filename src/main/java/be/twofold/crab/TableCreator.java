package be.twofold.crab;

import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

public final class TableCreator {

    private final Connection connection;
    private final boolean withMetadata;

    public TableCreator(Connection connection, boolean withMetadata) {
        this.connection = Objects.requireNonNull(connection);
        this.withMetadata = withMetadata;
    }

    void createTable(Path path, String filename) throws SQLException {
        System.out.println("Create " + path);

        String sql = createSql(filename);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private String createSql(String filename) {
        String tableName = Tables.getPgName(filename);
        String columns = Tables.getColumnsFor(filename, withMetadata).stream()
            .map(c -> c.getPgName() + " " + c.getTypeString())
            .collect(Collectors.joining(", "));

        return "create table if not exists " + tableName + " (" + columns + ")";
    }

}
