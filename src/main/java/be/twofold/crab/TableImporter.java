package be.twofold.crab;

import be.twofold.crab.model.*;
import be.twofold.crab.utils.*;
import be.twofold.tinydbf.*;

import java.io.*;
import java.nio.file.*;
import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.stream.*;

public final class TableImporter {

    private static final ZoneId BRUSSELS = ZoneId.of("Europe/Brussels");

    private final Connection connection;
    private final boolean withMetadata;

    public TableImporter(Connection connection, boolean withMetadata) {
        this.connection = Objects.requireNonNull(connection);
        this.withMetadata = withMetadata;
    }

    void importFile(Path path, String filename) throws IOException, SQLException {
        System.out.println("Importing " + path);

        List<Column> columns = Tables.getColumnsFor(filename, withMetadata);
        Map<String, Mapper> mappers = createMappers(columns);

        ProgressMonitor monitor = new ProgressMonitor();

        try (DbfReader reader = new DbfReader(Files.newInputStream(path))) {
            String sql = insertSql(filename);

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

    private String insertSql(String filename) {
        String tableName = Tables.getPgName(filename);
        List<Column> columns = Tables.getColumnsFor(filename, withMetadata);
        String parameters = columns.stream()
            .map(Column::getPgName)
            .collect(Collectors.joining(", "));

        String values = columns.stream()
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
            int parameterIndex = i + 1;
            Mapper mapper = mapper(parameterIndex, column.getSqlType());
            if (column.isNullable()) {
                mapper = nullSafe(mapper, parameterIndex, column.getSqlType());
            }
            mappers.put(column.getName(), mapper);
        }
        return Map.copyOf(mappers);
    }

    private Mapper mapper(int index, int sqlType) {
        switch (sqlType) {
            case Types.DATE:
                return (stmt, value) -> stmt.setDate(index, Date.valueOf(value.asDate()));
            case Types.FLOAT:
                return (stmt, value) -> stmt.setFloat(index, value.asNumeric().floatValue());
            case Types.INTEGER:
                return (stmt, value) -> stmt.setInt(index, value.asNumeric().intValue());
            case Types.SMALLINT:
                return (stmt, value) -> stmt.setShort(index, value.asNumeric().shortValue());
            case Types.TIMESTAMP:
                return (stmt, value) -> stmt.setTimestamp(index, parseTimestamp(value.asCharacter()));
            case Types.VARCHAR:
                return (stmt, value) -> stmt.setString(index, value.asCharacter());
            default:
                throw new IllegalStateException("Unexpected type: " + sqlType);
        }
    }

    private Timestamp parseTimestamp(String s) {
        int year = Integer.parseInt(s, 0, 4, 10);
        int month = Integer.parseInt(s, 4, 6, 10);
        int dayOfMonth = Integer.parseInt(s, 6, 8, 10);
        int hour = Integer.parseInt(s, 9, 11, 10);
        int minute = Integer.parseInt(s, 11, 13, 10);
        int second = Integer.parseInt(s, 13, 15, 10);
        LocalDateTime instant = LocalDateTime
            .of(year, month, dayOfMonth, hour, minute, second)
            .atZone(BRUSSELS)
            .withZoneSameInstant(ZoneOffset.UTC)
            .toLocalDateTime();

        return Timestamp.valueOf(instant);
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
