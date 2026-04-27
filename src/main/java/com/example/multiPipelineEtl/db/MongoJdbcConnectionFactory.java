package com.example.multiPipelineEtl.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class MongoJdbcConnectionFactory {
    private MongoJdbcConnectionFactory() {
    }

    public static Connection openConnection(MongoJdbcConfig config) throws SQLException {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }

        if (config.getUser().isEmpty()) {
            return DriverManager.getConnection(config.getJdbcUrl());
        }
        return DriverManager.getConnection(config.getJdbcUrl(), config.getUser(), config.getPassword());
    }
}
