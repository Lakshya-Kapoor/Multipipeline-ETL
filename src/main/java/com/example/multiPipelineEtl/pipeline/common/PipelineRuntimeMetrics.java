package com.example.multiPipelineEtl.pipeline.common;

public class PipelineRuntimeMetrics {
    private final long runtimeMillis;
    private final long totalRecordsProcessed;
    private final long malformedRecordCount;
    private final int nonEmptyBatchCount;
    private final double averageBatchSize;

    public PipelineRuntimeMetrics(
        long runtimeMillis,
        long totalRecordsProcessed,
        long malformedRecordCount,
        int nonEmptyBatchCount,
        double averageBatchSize
    ) {
        this.runtimeMillis = runtimeMillis;
        this.totalRecordsProcessed = totalRecordsProcessed;
        this.malformedRecordCount = malformedRecordCount;
        this.nonEmptyBatchCount = nonEmptyBatchCount;
        this.averageBatchSize = averageBatchSize;
    }

    public long getRuntimeMillis() {
        return runtimeMillis;
    }

    public long getTotalRecordsProcessed() {
        return totalRecordsProcessed;
    }

    public long getMalformedRecordCount() {
        return malformedRecordCount;
    }

    public int getNonEmptyBatchCount() {
        return nonEmptyBatchCount;
    }

    public double getAverageBatchSize() {
        return averageBatchSize;
    }
}
