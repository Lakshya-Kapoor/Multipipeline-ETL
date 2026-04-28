package com.example.multiPipelineEtl.db;

public class MongoDriverConfig {
    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String password;
    private final String authSource;

    public MongoDriverConfig(String host, int port, String database, String user, String password, String authSource) {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("host cannot be null or empty");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        if (database == null || database.trim().isEmpty()) {
            throw new IllegalArgumentException("database cannot be null or empty");
        }
        if (user == null) {
            throw new IllegalArgumentException("user cannot be null");
        }
        if (password == null) {
            throw new IllegalArgumentException("password cannot be null");
        }
        if (authSource == null || authSource.trim().isEmpty()) {
            throw new IllegalArgumentException("authSource cannot be null or empty");
        }
        
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
        this.authSource = authSource;
    }

    public static MongoDriverConfig fromEnvironment() {
        String host = envOrDefault("MONGO_HOST", "localhost");
        int port = envAsInt("MONGO_PORT", 27017);
        String database = envRequired("MONGO_DATABASE");
        String user = envOrDefault("MONGO_USER", "");
        String password = envOrDefault("MONGO_PASSWORD", "");
        String authSource = envOrDefault("MONGO_AUTH_SOURCE", "admin");
        
        return new MongoDriverConfig(host, port, database, user, password, authSource);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getAuthSource() {
        return authSource;
    }

    public String getConnectionString() {
        if (user.isEmpty()) {
            return String.format("mongodb://%s:%d/?authSource=%s", host, port, authSource);
        }
        return String.format("mongodb://%s:%s@%s:%d/?authSource=%s", user, password, host, port, authSource);
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return value;
    }

    private static String envRequired(String key) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Required environment variable not set: " + key);
        }
        return value;
    }

    private static int envAsInt(String key, int defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid integer value for " + key + ": " + value);
        }
    }
}
