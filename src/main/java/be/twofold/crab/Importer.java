package be.twofold.crab;

import be.twofold.crab.utils.*;
import be.twofold.tinydbf.*;

import java.io.*;
import java.math.*;
import java.nio.file.*;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

public final class Importer {

    private static final Set<String> MetadataColumns = Set.of(
        "BEGINDATUM",
        "EINDDATUM",
        "BEGINTIJD",
        "BEGINORG",
        "BEGINBEW"
    );

    private final Connection connection;

    public Importer(Connection connection) {
        this.connection = Objects.requireNonNull(connection);
    }

    // region Create table

    void createTable(Path path) throws IOException, SQLException {
        System.out.println("Create " + path);

        DbfHeader header = readHeader(path);
        String sql = createSql(header, nameWithoutExtension(path.getFileName()));
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private DbfHeader readHeader(Path path) throws IOException {
        try (DbfReader reader = new DbfReader(Files.newInputStream(path))) {
            return reader.getHeader();
        }
    }

    private String createSql(DbfHeader header, String fileName) {
        String columns = StreamSupport.stream(header.spliterator(), false)
            .filter(f -> !MetadataColumns.contains(f.getName()))
            .map(f -> f.getName() + " " + getType(f))
            .collect(Collectors.joining(", "));

        return "create table if not exists " + fileName + " (" + columns + ")";
    }

    private String getType(DbfField field) {
        switch (field.getType()) {
            case Char:
                return "varchar(" + field.getLength() + ")";
            case Date:
                return "date";
            case Floating:
                return "real";
            case Logical:
                return "boolean";
            case Numeric:
                String decimalCount = field.getDecimalCount() > 0 ? "," + field.getDecimalCount() : "";
                return ("decimal(" + field.getLength()) + decimalCount + ")";
            default:
                throw new IllegalArgumentException();
        }
    }

    // endregion

    // region Import data

    void importFile(Path path) throws IOException, SQLException {
        System.out.println("Importing " + path);

        try (DbfReader reader = new DbfReader(Files.newInputStream(path))) {
            DbfHeader header = reader.getHeader();
            String sql = insertSql(header, nameWithoutExtension(path.getFileName()));

            ProgressMonitor monitor = new ProgressMonitor();
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                while (reader.hasNext()) {
                    DbfRecord record = reader.next();
                    recordToStmt(record, stmt, header);

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

    private String insertSql(DbfHeader header, String filename) {
        String parameters = StreamSupport.stream(header.spliterator(), false)
            .filter(f -> !MetadataColumns.contains(f.getName()))
            .map(DbfField::getName)
            .collect(Collectors.joining(", "));

        String values = StreamSupport.stream(header.spliterator(), false)
            .filter(f -> !MetadataColumns.contains(f.getName()))
            .map(__ -> "?")
            .collect(Collectors.joining(", "));

        return "insert into " + filename + " (" + parameters + ") values (" + values + ");";
    }

    private void recordToStmt(DbfRecord record, PreparedStatement statement, DbfHeader header) throws SQLException {
        int parameterIndex = 1;
        for (DbfField field : header) {
            if (MetadataColumns.contains(field.getName())) {
                continue;
            }
            DbfValue value = record.get(field.getName());
            if (value.isCharacter()) {
                statement.setString(parameterIndex++, value.asCharacter());
            } else if (value.isDate()) {
                statement.setDate(parameterIndex++, Date.valueOf(value.asDate()));
            } else if (value.isLogical()) {
                statement.setBoolean(parameterIndex++, value.asLogical());
            } else if (value.isNull()) {
                statement.setNull(parameterIndex++, getSqlType(field.getType()));
            } else if (value.isNumeric()) {
                statement.setBigDecimal(parameterIndex++, new BigDecimal(value.asNumeric().toString()));
            }
        }
    }

    private int getSqlType(DbfType type) {
        switch (type) {
            case Char:
                return Types.VARCHAR;
            case Date:
                return Types.DATE;
            case Floating:
                return Types.FLOAT;
            case Logical:
                return Types.BOOLEAN;
            case Numeric:
                return Types.DECIMAL;
            default:
                throw new IllegalArgumentException();
        }
    }

    private void commit(PreparedStatement statement) throws SQLException {
        statement.executeBatch();
        connection.commit();
    }

    // endregion

    private String nameWithoutExtension(Path path) {
        Path fileName = path.getFileName();
        if (fileName == null) {
            return null;
        }

        String fileNameString = fileName.toString();
        int index = fileNameString.lastIndexOf('.');
        return index == -1 ? fileNameString : fileNameString.substring(0, index);
    }

}
