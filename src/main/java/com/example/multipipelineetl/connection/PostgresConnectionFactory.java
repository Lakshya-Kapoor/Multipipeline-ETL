package com.example.multipipelineetl.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresConnectionFactory {
    public Connection getConnection() throws SQLException {
        String url = envRequired("PG_URL");
        String user = envRequired("PG_USER");
        String password = envRequired("PG_PASSWORD");
        return DriverManager.getConnection(url, user, password);
    }

    private String envRequired(String key) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Missing required environment variable: " + key);
        }
        return value;
    }
}

