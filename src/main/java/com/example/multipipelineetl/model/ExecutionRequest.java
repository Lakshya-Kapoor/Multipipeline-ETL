package com.example.multipipelineetl.model;

public class ExecutionRequest {
    private final PipelineType pipelineType;
    private final QueryType queryType;
    private final int batchSize;
    private final String datasetPath;

    public ExecutionRequest(PipelineType pipelineType, QueryType queryType, int batchSize, String datasetPath) {
        this.pipelineType = pipelineType;
        this.queryType = queryType;
        this.batchSize = batchSize;
        this.datasetPath = datasetPath;
    }

    public PipelineType getPipelineType() {
        return pipelineType;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getDatasetPath() {
        return datasetPath;
    }
}
