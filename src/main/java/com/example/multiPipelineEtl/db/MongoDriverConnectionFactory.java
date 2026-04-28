package com.example.multiPipelineEtl.db;

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.UuidRepresentation;

import java.util.Arrays;

public final class MongoDriverConnectionFactory {
    private MongoDriverConnectionFactory() {
    }

    public static MongoClient openConnection(MongoDriverConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }

        if (config.getUser().isEmpty()) {
            return MongoClients.create(config.getConnectionString());
        }

        MongoCredential credential = MongoCredential.createScramSha1Credential(
            config.getUser(),
            config.getAuthSource(),
            config.getPassword().toCharArray()
        );

        return MongoClients.create(
            com.mongodb.MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                    builder.hosts(Arrays.asList(
                        new ServerAddress(config.getHost(), config.getPort())
                    ))
                )
                .credential(credential)
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build()
        );
    }
}
