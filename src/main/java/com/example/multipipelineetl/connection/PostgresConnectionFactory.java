package com.example.multipipelineetl.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresConnectionFactory {
    public Connection getConnection() throws SQLException {
        String host = envRequired("PG_HOST");
        String port = envRequired("PG_PORT");
        String database = envRequired("PG_DB");
        String user = envRequired("PG_USER");
        String password = envRequired("PG_PASSWORD");
        
        String url = String.format("jdbc:postgresql://%s:%s/%s", host, port, database);
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

