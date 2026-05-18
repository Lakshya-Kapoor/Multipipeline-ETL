package com.example.multipipelineetl.model;

public class BatchMetadata {
    private final long runId;
    private final int queryId;
    private final int batchId;
    private final long recordsProcessed;
    private final long malformedRecords;
    private final long batchRuntimeMs;

    public BatchMetadata(long runId, int queryId, int batchId, long recordsProcessed, long malformedRecords, long batchRuntimeMs) {
        this.runId = runId;
        this.queryId = queryId;
        this.batchId = batchId;
        this.recordsProcessed = recordsProcessed;
        this.malformedRecords = malformedRecords;
        this.batchRuntimeMs = batchRuntimeMs;
    }

    public long getRunId() {
        return runId;
    }

    public int getBatchId() {
        return batchId;
    }

    public int getQueryId() {
        return queryId;
    }

    public long getRecordsProcessed() {
        return recordsProcessed;
    }

    public long getMalformedRecords() {
        return malformedRecords;
    }

    public long getBatchRuntimeMs() {
        return batchRuntimeMs;
    }
}
