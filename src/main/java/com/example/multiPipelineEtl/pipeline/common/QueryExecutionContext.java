package com.example.multiPipelineEtl.pipeline.common;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;

public class QueryExecutionContext {
    private final String executionChoiceName;
    private final String pipelineName;
    private final int batchSize;
    private final Path inputFilePath;
    private final Connection connection;
    private final Instant executionTime;

    public QueryExecutionContext(
        String executionChoiceName,
        String pipelineName,
        int batchSize,
        Path inputFilePath,
        Connection connection,
        Instant executionTime
    ) {
        if (executionChoiceName == null || executionChoiceName.trim().isEmpty()) {
            throw new IllegalArgumentException("executionChoiceName cannot be null or empty");
        }
        if (pipelineName == null || pipelineName.trim().isEmpty()) {
            throw new IllegalArgumentException("pipelineName cannot be null or empty");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }
        if (inputFilePath == null) {
            throw new IllegalArgumentException("inputFilePath cannot be null");
        }
        if (connection == null) {
            throw new IllegalArgumentException("connection cannot be null");
        }
        if (executionTime == null) {
            throw new IllegalArgumentException("executionTime cannot be null");
        }

        this.executionChoiceName = executionChoiceName;
        this.pipelineName = pipelineName;
        this.batchSize = batchSize;
        this.inputFilePath = inputFilePath;
        this.connection = connection;
        this.executionTime = executionTime;
    }

    public String getExecutionChoiceName() {
        return executionChoiceName;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Path getInputFilePath() {
        return inputFilePath;
    }

    public Connection getConnection() {
        return connection;
    }

    public Instant getExecutionTime() {
        return executionTime;
    }
}
