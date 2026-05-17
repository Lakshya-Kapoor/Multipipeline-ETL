package com.example.multipipelineetl.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveConnectionFactory {
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private String hiveUrl;

    public HiveConnectionFactory() {
        String hiveHost = System.getenv("HIVE_HOST");
        String hivePort = System.getenv("HIVE_PORT");
        String hiveDatabase = System.getenv("HIVE_DATABASE");

        if (hiveHost == null) hiveHost = "localhost";
        if (hivePort == null) hivePort = "10000";
        if (hiveDatabase == null) hiveDatabase = "default";

        this.hiveUrl = String.format("jdbc:hive2://%s:%s/%s", hiveHost, hivePort, hiveDatabase);
    }

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName(HIVE_DRIVER);
        return DriverManager.getConnection(hiveUrl);
    }
}
