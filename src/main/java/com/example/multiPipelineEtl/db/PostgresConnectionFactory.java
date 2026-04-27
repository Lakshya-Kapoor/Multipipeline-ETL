package com.example.multiPipelineEtl.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class PostgresConnectionFactory {
    private PostgresConnectionFactory() {
    }

    public static Connection openConnection(PostgresConfig config) throws SQLException {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        return DriverManager.getConnection(config.toJdbcUrl(), config.getUser(), config.getPassword());
    }
}
