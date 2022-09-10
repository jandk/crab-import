package be.twofold.crab;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

public final class Main {

    public static void main(String[] args) throws IOException, SQLException {
        if (args.length != 1) {
            System.out.println("Usage: CrabImport <path to crab data>");
            System.exit(1);
        }

        importFiles(scanFiles(Paths.get(args[0])));
    }

    private static void importFiles(List<Path> filePaths) throws SQLException, IOException {
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/crab?reWriteBatchedInserts=true", "crab", "crab")) {
            conn.setAutoCommit(false);
            TableCreator creator = new TableCreator(conn, false);
            TableImporter importer = new TableImporter(conn);
            for (Path filePath : filePaths) {
                creator.createTable(filePath, tableName(filePath));
            }
            for (Path filePath : filePaths) {
                importer.importFile(filePath);
            }
        }
    }

    private static List<Path> scanFiles(Path path) throws IOException {
        try (Stream<Path> list = Files.list(path)) {
            return list
                .filter(p -> p.toString().endsWith(".dbf"))
                .collect(Collectors.toList());
        }
    }

    private static String tableName(Path path) {
        String filename = path.getFileName().toString();
        int index = filename.lastIndexOf('.');
        return index == -1 ? filename : filename.substring(0, index);
    }

}
