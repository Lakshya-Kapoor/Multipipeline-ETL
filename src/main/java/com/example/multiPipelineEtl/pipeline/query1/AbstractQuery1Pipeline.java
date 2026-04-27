package com.example.multiPipelineEtl.pipeline.query1;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import com.example.multiPipelineEtl.pipeline.common.PipelineRuntimeMetrics;
import com.example.multiPipelineEtl.pipeline.common.QueryExecutionContext;
import com.example.multiPipelineEtl.pipeline.contracts.Query1Pipeline;

public abstract class AbstractQuery1Pipeline implements Query1Pipeline {
    private static final int QUERY_NUMBER = 1;
    private static final String QUERY_NAME = "Query1_DailyTrafficSummary";
    private long currentBatchRecordCount = 0L;
    private long currentBatchMalformedCount = 0L;

    @Override
    public final void run(int numberOfBatches, QueryExecutionContext context) throws IOException, SQLException {
        if (numberOfBatches <= 0) {
            throw new IllegalArgumentException("numberOfBatches must be greater than zero");
        }
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }

        long overallStartMillis = System.currentTimeMillis();
        long totalRecordsProcessed = 0L;
        long totalMalformedRecords = 0L;
        int nonEmptyBatchCount = 0;

        for (int batchId = 1; batchId <= numberOfBatches; batchId++) {
            resetCurrentBatchStats();
            long batchStartMillis = System.currentTimeMillis();

            extractAndParse(context, batchId);
            groupAndAggregate(context, batchId);
            load(context, batchId);

            long recordsInBatch = getCurrentBatchRecordCount();
            long malformedInBatch = getCurrentBatchMalformedCount();
            totalRecordsProcessed += recordsInBatch;
            totalMalformedRecords += malformedInBatch;
            if (recordsInBatch > 0) {
                nonEmptyBatchCount++;
            }

            long batchRuntimeMillis = System.currentTimeMillis() - batchStartMillis;
            double averageBatchSize = nonEmptyBatchCount == 0
                ? 0.0
                : (double) totalRecordsProcessed / (double) nonEmptyBatchCount;

            writeBatchMetadata(
                context,
                batchId,
                batchRuntimeMillis,
                recordsInBatch,
                malformedInBatch,
                averageBatchSize
            );
        }

        long overallRuntimeMillis = System.currentTimeMillis() - overallStartMillis;
        double averageBatchSize = nonEmptyBatchCount == 0
            ? 0.0
            : (double) totalRecordsProcessed / (double) nonEmptyBatchCount;

        PipelineRuntimeMetrics pipelineRuntimeMetrics = new PipelineRuntimeMetrics(
            overallRuntimeMillis,
            totalRecordsProcessed,
            totalMalformedRecords,
            nonEmptyBatchCount,
            averageBatchSize
        );
        onRunCompleted(context, pipelineRuntimeMetrics);
    }

    protected abstract void extractAndParse(QueryExecutionContext context, int batchId) throws IOException, SQLException;

    protected abstract void groupAndAggregate(QueryExecutionContext context, int batchId) throws SQLException;

    protected abstract void load(QueryExecutionContext context, int batchId) throws SQLException;

    protected final void setCurrentBatchStats(long recordsProcessed, long malformedRecords) {
        if (recordsProcessed < 0L) {
            throw new IllegalArgumentException("recordsProcessed cannot be negative");
        }
        if (malformedRecords < 0L) {
            throw new IllegalArgumentException("malformedRecords cannot be negative");
        }
        currentBatchRecordCount = recordsProcessed;
        currentBatchMalformedCount = malformedRecords;
    }

    protected void onRunCompleted(QueryExecutionContext context, PipelineRuntimeMetrics metrics) throws SQLException {
        String sql = "INSERT INTO execution_metadata "
            + "(execution_choice, pipeline_name, query_number, query_name, batch_id, batch_size, "
            + "records_processed, malformed_records, runtime_millis, average_batch_size, executed_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement statement = context.getConnection().prepareStatement(sql)) {
            statement.setString(1, context.getExecutionChoiceName());
            statement.setString(2, context.getPipelineName());
            statement.setInt(3, QUERY_NUMBER);
            statement.setString(4, QUERY_NAME);
            statement.setNull(5, Types.INTEGER);
            statement.setInt(6, context.getBatchSize());
            statement.setLong(7, metrics.getTotalRecordsProcessed());
            statement.setLong(8, metrics.getMalformedRecordCount());
            statement.setLong(9, metrics.getRuntimeMillis());
            statement.setDouble(10, metrics.getAverageBatchSize());
            statement.setTimestamp(11, Timestamp.from(context.getExecutionTime()));
            statement.executeUpdate();
        }
    }

    private void writeBatchMetadata(
        QueryExecutionContext context,
        int batchId,
        long runtimeMillis,
        long recordsProcessed,
        long malformedRecords,
        double averageBatchSize
    ) throws SQLException {
        String sql = "INSERT INTO execution_metadata "
            + "(execution_choice, pipeline_name, query_number, query_name, batch_id, batch_size, "
            + "records_processed, malformed_records, runtime_millis, average_batch_size, executed_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement statement = context.getConnection().prepareStatement(sql)) {
            statement.setString(1, context.getExecutionChoiceName());
            statement.setString(2, context.getPipelineName());
            statement.setInt(3, QUERY_NUMBER);
            statement.setString(4, QUERY_NAME);
            statement.setInt(5, batchId);
            statement.setInt(6, context.getBatchSize());
            statement.setLong(7, recordsProcessed);
            statement.setLong(8, malformedRecords);
            statement.setLong(9, runtimeMillis);
            statement.setDouble(10, averageBatchSize);
            statement.setTimestamp(11, Timestamp.from(context.getExecutionTime()));
            statement.executeUpdate();
        }
    }

    private void resetCurrentBatchStats() {
        currentBatchRecordCount = 0L;
        currentBatchMalformedCount = 0L;
    }

    private long getCurrentBatchRecordCount() {
        return currentBatchRecordCount;
    }

    private long getCurrentBatchMalformedCount() {
        return currentBatchMalformedCount;
    }
}
