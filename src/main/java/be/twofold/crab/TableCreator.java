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

    void createTable(Path path, String tableName) throws SQLException {
        System.out.println("Create " + path);

        String sql = createSql(tableName);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            connection.commit();
        }
    }

    private String createSql(String tableName) {
        String columns = Tables.getColumnsFor(tableName, withMetadata).stream()
            .map(c -> c.getPgName() + " " + c.getTypeString())
            .collect(Collectors.joining(", "));

        return "create table if not exists " + tableName + " (" + columns + ")";
    }

}
