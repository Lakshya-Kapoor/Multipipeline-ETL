package com.example.multipipelineetl.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveConnectionFactory {
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private String hiveUrl;

    public HiveConnectionFactory() {
        String hiveHost = envRequired("HIVE_HOST");
        String hivePort = envRequired("HIVE_PORT");
        String hiveDatabase = envRequired("HIVE_DATABASE");

        this.hiveUrl = String.format("jdbc:hive2://%s:%s/%s;user=sahas", hiveHost, hivePort, hiveDatabase);
    }

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName(HIVE_DRIVER);
        return DriverManager.getConnection(hiveUrl);
    }

    private String envRequired(String key) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Missing required environment variable: " + key);
        }
        return value;
    }
}
