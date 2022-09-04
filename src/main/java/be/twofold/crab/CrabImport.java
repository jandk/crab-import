package be.twofold.crab;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
