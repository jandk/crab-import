package be.twofold.crab;

import be.twofold.tinydbf.DbfField;
import be.twofold.tinydbf.DbfHeader;
import be.twofold.tinydbf.DbfReader;
import be.twofold.tinydbf.DbfRecord;
import be.twofold.tinydbf.DbfType;
import be.twofold.tinydbf.DbfValue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public final class Importer {

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
        String columns = IntStream.range(0, header.getFieldCount())
            .mapToObj(i -> {
                DbfField field = header.getField(i);
                return field.getName() + " " + getType(field);
            })
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

            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                int index = 0;
                while (reader.hasNext()) {
                    DbfRecord record = reader.next();
                    recordToStmt(record, stmt, header);
                    stmt.addBatch();

                    if (++index % 100_000 == 0) {
                        commit(connection, stmt, index);
                    }
                }
                commit(connection, stmt, header.getNumberOfRecords());
            }
        }
        System.out.println("-".repeat(50));
    }

    private String insertSql(DbfHeader header, String filename) {
        String parameters = StreamSupport.stream(header.spliterator(), false)
            .map(DbfField::getName)
            .collect(Collectors.joining(", "));

        String values = IntStream.range(0, header.getFieldCount())
            .mapToObj(__ -> "?")
            .collect(Collectors.joining(", "));

        return "insert into " + filename + " (" + parameters + ") values (" + values + ");";
    }

    private void recordToStmt(DbfRecord record, PreparedStatement statement, DbfHeader header) throws SQLException {
        for (int i = 0; i < record.size(); i++) {
            DbfValue value = record.get(i);
            int parameterIndex = i + 1;
            if (value.isCharacter()) {
                statement.setString(parameterIndex, value.asCharacter());
            } else if (value.isDate()) {
                statement.setDate(parameterIndex, Date.valueOf(value.asDate()));
            } else if (value.isLogical()) {
                statement.setBoolean(parameterIndex, value.asLogical());
            } else if (value.isNull()) {
                statement.setNull(parameterIndex, getSqlType(header.getField(i).getType()));
            } else if (value.isNumeric()) {
                statement.setBigDecimal(parameterIndex, new BigDecimal(value.asNumeric().toString()));
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

    private void commit(Connection connection, PreparedStatement statement, int numberOfRecords) throws SQLException {
        statement.executeBatch();
        connection.commit();
        System.out.println("Imported " + numberOfRecords + " records");
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
