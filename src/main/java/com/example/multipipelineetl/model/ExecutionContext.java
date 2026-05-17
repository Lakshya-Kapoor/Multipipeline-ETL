package com.example.multipipelineetl.model;

public class ExecutionContext {
    private final long runId;
    private final ExecutionRequest request;

    public ExecutionContext(long runId, ExecutionRequest request) {
        this.runId = runId;
        this.request = request;
    }

    public long getRunId() {
        return runId;
    }

    public ExecutionRequest getRequest() {
        return request;
    }
}
