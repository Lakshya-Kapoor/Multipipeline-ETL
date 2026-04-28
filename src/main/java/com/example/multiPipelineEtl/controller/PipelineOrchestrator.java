package com.example.multiPipelineEtl.controller;

import java.io.PrintStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;

import com.example.multiPipelineEtl.db.MongoDriverConfig;
import com.example.multiPipelineEtl.db.MongoDriverConnectionFactory;
import com.example.multiPipelineEtl.db.PostgresConfig;
import com.example.multiPipelineEtl.db.PostgresConnectionFactory;
import com.example.multiPipelineEtl.pipeline.common.QueryExecutionContext;
import com.example.multiPipelineEtl.pipeline.contracts.Query1Pipeline;
import com.example.multiPipelineEtl.pipeline.query1.MongoDbQuery1Pipeline;
import com.mongodb.client.MongoClient;

import java.sql.Connection;
import java.sql.SQLException;

public class PipelineOrchestrator {
    private static final String DEFAULT_INPUT_PATH = "data/access_log_Aug95";
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private final PrintStream out;

    public PipelineOrchestrator(PrintStream out) {
        if (out == null) {
            throw new IllegalArgumentException("out cannot be null");
        }
        this.out = out;
    }

    public void execute(ExecutionRequest request) {
        out.println();
        out.println("Execution request accepted.");
        out.println("Execution choice: " + request.getExecutionChoice());
        out.println("Number of batches: " + request.getNumberOfBatches());
        out.println("Input dataset: " + DEFAULT_INPUT_PATH);

        if (request.getExecutionChoice() != ExecutionChoice.MONGODB) {
            out.println("Concrete execution is currently implemented only for MongoDB Query 1.");
            return;
        }

        PostgresConfig postgresConfig = PostgresConfig.fromEnvironment();
        MongoDriverConfig mongoDriverConfig = MongoDriverConfig.fromEnvironment();

        MongoClient mongoClient = MongoDriverConnectionFactory.openConnection(mongoDriverConfig);
        try (Connection postgresConnection = PostgresConnectionFactory.openConnection(postgresConfig)) {

            QueryExecutionContext executionContext = new QueryExecutionContext(
                request.getExecutionChoice().name(),
                "MongoDBQuery1Pipeline",
                DEFAULT_BATCH_SIZE,
                Paths.get(DEFAULT_INPUT_PATH),
                postgresConnection,
                Instant.now()
            );

            Query1Pipeline query1Pipeline = new MongoDbQuery1Pipeline(mongoClient, mongoDriverConfig.getDatabase());
            query1Pipeline.run(request.getNumberOfBatches(), executionContext);
            out.println("MongoDB Query 1 execution completed.");
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to execute MongoDB Query 1 pipeline.", ex);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to read input dataset for MongoDB Query 1.", ex);
        } finally {
            mongoClient.close();
        }
    }
}
