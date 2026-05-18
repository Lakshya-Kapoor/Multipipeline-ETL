package com.example.multipipelineetl.connection;

import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class MongoConnectionFactory {
    private static MongoClient mongoClient;
    private static MongoDatabase mongoDatabase;

    public static MongoDatabase getDatabase() {
        if (mongoClient == null) {
            initializeConnection();
        }
        return mongoDatabase;
    }

    public static MongoClient getClient() {
        if (mongoClient == null) {
            initializeConnection();
        }
        return mongoClient;
    }

    private static void initializeConnection() {
        String host = envRequired("MONGO_HOST");
        String port = envRequired("MONGO_PORT");
        String database = envRequired("MONGO_DATABASE");
        String user = envOptional("MONGO_USER");
        String password = envOptional("MONGO_PASSWORD");
        String authSource = envOptional("MONGO_AUTH_SOURCE");

        String connectionString = String.format("mongodb://%s:%s", host, port);
        
        if (user != null && !user.isEmpty() && password != null && !password.isEmpty()) {
            String auth = String.format("%s:%s@", user, password);
            connectionString = connectionString.replace("mongodb://", "mongodb://" + auth);
            if (authSource != null && !authSource.isEmpty()) {
                connectionString += "/?authSource=" + authSource;
            }
        }

        mongoClient = MongoClients.create(connectionString);
        mongoDatabase = mongoClient.getDatabase(database);
    }

    private static String envRequired(String key) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Missing required environment variable: " + key);
        }
        return value;
    }

    private static String envOptional(String key) {
        String value = System.getenv(key);
        return (value == null || value.trim().isEmpty()) ? null : value;
    }

    public static void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
            mongoDatabase = null;
        }
    }
}
