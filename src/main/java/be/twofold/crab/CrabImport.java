package be.twofold.crab;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

public class CrabImport {
    public static void main(String[] args) throws IOException, SQLException {
        if (args.length != 1) {
            System.out.println("Usage: CrabImport <path to crab data>");
            System.exit(1);
        }

        List<Path> filePaths = scanFiles(Paths.get(args[0]));

        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost/crab?reWriteBatchedInserts=true", "crab", "crab")) {
            connection.setAutoCommit(false);
            Importer importer = new Importer(connection);
            for (Path filePath : filePaths) {
                importer.createTable(filePath);
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

}
