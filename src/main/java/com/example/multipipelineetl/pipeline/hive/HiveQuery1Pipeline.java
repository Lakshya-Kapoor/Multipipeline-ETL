package com.example.multipipelineetl.pipeline.hive;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.HiveTableManager;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query1Result;
import com.example.multipipelineetl.persistence.QueryResultRepository;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class HiveQuery1Pipeline {
    private static final String TABLE_PREFIX = "hive_query1_batch_";
    private static final String VIEW_NAME = "hive_query1_transformed";

    public void execute(BatchFile batch, String absoluteBatchPath, ExecutionContext context, QueryResultRepository resultRepository,
                        Connection hiveConnection) throws Exception {
        String tableName = TABLE_PREFIX + batch.getBatchId();

        try {
            // Create external table with absolute path
            HiveTableManager.createExternalTable(hiveConnection, tableName, absoluteBatchPath);

            // Execute aggregation query
            List<Query1Result> results = executeAggregation(hiveConnection, tableName);

            // Load results to PostgreSQL
            resultRepository.insertQuery1(context.getRunId(), batch.getBatchId(), "HIVE", results);

        } finally {
            // Cleanup
            HiveTableManager.dropTable(hiveConnection, tableName);
        }
    }

    private List<Query1Result> executeAggregation(Connection hiveConnection, String tableName) throws Exception {
        String aggregationQuery = String.format(
                "SELECT\n" +
                "  CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(log_timestamp, 'dd/MMM/yyyy:HH:mm:ss Z'), 'yyyy-MM-dd') AS DATE) AS log_date,\n" +
                "  status_code,\n" +
                "  COUNT(*) AS request_count,\n" +
                "  SUM(CASE WHEN bytes_transferred = -1 THEN 0 ELSE bytes_transferred END) AS total_bytes\n" +
                "FROM %s\n" +
                "GROUP BY CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(log_timestamp, 'dd/MMM/yyyy:HH:mm:ss Z'), 'yyyy-MM-dd') AS DATE), status_code",
                tableName);

        List<Query1Result> results = new ArrayList<>();
        try (Statement stmt = hiveConnection.createStatement()) {
            ResultSet rs = stmt.executeQuery(aggregationQuery);
            while (rs.next()) {
                results.add(new Query1Result(
                        rs.getDate("log_date"),
                        rs.getInt("status_code"),
                        rs.getLong("request_count"),
                        rs.getLong("total_bytes")));
            }
        }
        return results;
    }
}
