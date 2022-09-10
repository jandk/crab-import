package be.twofold.crab;

import be.twofold.crab.model.*;
import be.twofold.crab.utils.*;
import be.twofold.tinydbf.*;

import java.io.*;
import java.nio.file.*;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

public final class TableImporter {

    private final Connection connection;
    private final boolean withMetadata;

    public TableImporter(Connection connection, boolean withMetadata) {
        this.connection = Objects.requireNonNull(connection);
        this.withMetadata = withMetadata;
    }

    void importFile(Path path, String tableName) throws IOException, SQLException {
        System.out.println("Importing " + path);

        List<Column> columns = Tables.getColumnsFor(tableName, withMetadata);
        Map<String, Mapper> mappers = createMappers(columns);

        ProgressMonitor monitor = new ProgressMonitor();

        try (DbfReader reader = new DbfReader(Files.newInputStream(path))) {
            String sql = insertSql(tableName);

            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                while (reader.hasNext()) {
                    DbfRecord record = reader.next();
                    for (Column column : columns) {
                        Mapper mapper = mappers.get(column.getName());
                        mapper.set(stmt, record.get(column.getName()));
                    }

                    stmt.addBatch();
                    int count = monitor.incrementCount();
                    if ((count % 1024) == 0) {
                        commit(stmt);
                    }
                }
                monitor.print();
                commit(stmt);
            }

        }
        System.out.println("-".repeat(50));
    }

    private String insertSql(String tableName) {
        String parameters = Tables.getColumnsFor(tableName, withMetadata).stream()
            .map(Column::getPgName)
            .collect(Collectors.joining(", "));

        String values = Tables.getColumnsFor(tableName, withMetadata).stream()
            .map(__ -> "?")
            .collect(Collectors.joining(", "));

        return "insert into " + tableName + " (" + parameters + ") values (" + values + ");";
    }

    private void commit(PreparedStatement statement) throws SQLException {
        statement.executeBatch();
        connection.commit();
    }

    private Map<String, Mapper> createMappers(List<Column> columns) {
        Map<String, Mapper> mappers = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            Mapper mapper = mapper(i + 1, column.getSqlType());
            if (column.isNullable()) {
                mapper = nullSafe(mapper, i + i, column.getSqlType());
            }
            mappers.put(column.getName(), mapper);
        }
        return Map.copyOf(mappers);
    }

    private Mapper mapper(int index, int sqlType) {
        return switch (sqlType) {
            case Types.DATE -> (stmt, value) -> stmt.setDate(index, Date.valueOf(value.asDate()));
            case Types.FLOAT -> (stmt, value) -> stmt.setFloat(index, value.asNumeric().floatValue());
            case Types.INTEGER -> (stmt, value) -> stmt.setInt(index, value.asNumeric().intValue());
            case Types.SMALLINT -> (stmt, value) -> stmt.setShort(index, value.asNumeric().shortValue());
            case Types.VARCHAR -> (stmt, value) -> stmt.setString(index, value.asCharacter());
            default -> throw new IllegalStateException("Unexpected type: " + sqlType);
        };
    }

    private Mapper nullSafe(Mapper mapper, int index, int sqlType) {
        return (stmt, value) -> {
            if (value.isNull()) {
                stmt.setNull(index, sqlType);
            } else {
                mapper.set(stmt, value);
            }
        };
    }

}
