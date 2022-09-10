package be.twofold.crab;

import be.twofold.tinydbf.*;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

public final class TableCreator {

    private static final Set<String> MetadataColumns = Set.of(
        "BEGINDATUM",
        "EINDDATUM",
        "BEGINTIJD",
        "BEGINORG",
        "BEGINBEW"
    );

    private final Connection connection;

    public TableCreator(Connection connection) {
        this.connection = Objects.requireNonNull(connection);
    }

    void createTable(Path path) throws IOException, SQLException {
        System.out.println("Create " + path);

        DbfHeader header = readHeader(path);
        String sql = createSql(header, nameWithoutExtension(path.getFileName()));
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            connection.commit();
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
