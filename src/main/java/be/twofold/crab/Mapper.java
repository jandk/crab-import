package be.twofold.crab;

import be.twofold.tinydbf.*;

import java.sql.*;

@FunctionalInterface
public interface Mapper {
    void set(PreparedStatement stmt, DbfValue value) throws SQLException;
}
