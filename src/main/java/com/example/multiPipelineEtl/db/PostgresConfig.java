package com.example.multiPipelineEtl.db;

public class PostgresConfig {
    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String password;

    public PostgresConfig(String host, int port, String database, String user, String password) {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("host cannot be null or empty");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("port must be greater than zero");
        }
        if (database == null || database.trim().isEmpty()) {
            throw new IllegalArgumentException("database cannot be null or empty");
        }
        if (user == null || user.trim().isEmpty()) {
            throw new IllegalArgumentException("user cannot be null or empty");
        }
        if (password == null) {
            throw new IllegalArgumentException("password cannot be null");
        }
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    public static PostgresConfig fromEnvironment() {
        String host = envOrDefault("PG_HOST", "localhost");
        int port = Integer.parseInt(envOrDefault("PG_PORT", "5432"));
        String database = envOrDefault("PG_DB", "etl_reports");
        String user = envOrDefault("PG_USER", "postgres");
        String password = envOrDefault("PG_PASSWORD", "");
        return new PostgresConfig(host, port, database, user, password);
    }

    public String toJdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return value;
    }
}
