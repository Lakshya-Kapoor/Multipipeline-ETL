package com.example.multiPipelineEtl.db;

public class MongoJdbcConfig {
    private final String jdbcUrl;
    private final String user;
    private final String password;

    public MongoJdbcConfig(String jdbcUrl, String user, String password) {
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("jdbcUrl cannot be null or empty");
        }
        if (user == null) {
            throw new IllegalArgumentException("user cannot be null");
        }
        if (password == null) {
            throw new IllegalArgumentException("password cannot be null");
        }
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    public static MongoJdbcConfig fromEnvironment() {
        String jdbcUrl = envOrDefault("MONGO_JDBC_URL", "jdbc:mongodb://localhost:27017/etl");
        String user = envOrDefault("MONGO_JDBC_USER", "");
        String password = envOrDefault("MONGO_JDBC_PASSWORD", "");
        return new MongoJdbcConfig(jdbcUrl, user, password);
    }

    public String getJdbcUrl() {
        return jdbcUrl;
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
