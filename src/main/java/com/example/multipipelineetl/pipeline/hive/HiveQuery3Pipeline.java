package com.example.multipipelineetl.pipeline.hive;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.HiveTableManager;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query3Result;
import com.example.multipipelineetl.persistence.QueryResultRepository;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class HiveQuery3Pipeline {
    private static final String TABLE_PREFIX = "hive_query3_batch_";
    private static final String VIEW_NAME = "hive_query3_transformed";

    public void execute(BatchFile batch, String absoluteBatchPath, ExecutionContext context, QueryResultRepository resultRepository,
                        Connection hiveConnection) throws Exception {
        String tableName = TABLE_PREFIX + batch.getBatchId();

        try {
            // Create external table with absolute path
            HiveTableManager.createExternalTable(hiveConnection, tableName, absoluteBatchPath);

            // Execute aggregation query
            List<Query3Result> results = executeAggregation(hiveConnection, tableName);

            // Load results to PostgreSQL
            resultRepository.insertQuery3(context.getRunId(), batch.getBatchId(), "HIVE", results);

        } finally {
            // Cleanup
            HiveTableManager.dropTable(hiveConnection, tableName);
        }
    }

    private List<Query3Result> executeAggregation(Connection hiveConnection, String tableName) throws Exception {
        String aggregationQuery = String.format(
                "SELECT\n" +
                "  CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(log_timestamp, 'dd/MMM/yyyy:HH:mm:ss Z'), 'yyyy-MM-dd') AS DATE) AS log_date,\n" +
                "  HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(log_timestamp, 'dd/MMM/yyyy:HH:mm:ss Z'))) AS log_hour,\n" +
                "  SUM(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 ELSE 0 END) AS error_request_count,\n" +
                "  COUNT(*) AS total_request_count,\n" +
                "  SUM(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 ELSE 0 END) / COUNT(*) AS error_rate,\n" +
                "  COUNT(DISTINCT CASE WHEN status_code >= 400 AND status_code <= 599 THEN host END) AS distinct_error_hosts\n" +
                "FROM %s\n" +
                "GROUP BY CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(log_timestamp, 'dd/MMM/yyyy:HH:mm:ss Z'), 'yyyy-MM-dd') AS DATE), " +
                "HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(log_timestamp, 'dd/MMM/yyyy:HH:mm:ss Z')))",
                tableName);

        List<Query3Result> results = new ArrayList<>();
        try (Statement stmt = hiveConnection.createStatement()) {
            ResultSet rs = stmt.executeQuery(aggregationQuery);
            while (rs.next()) {
                results.add(new Query3Result(
                        rs.getDate("log_date"),
                        rs.getInt("log_hour"),
                        rs.getLong("error_request_count"),
                        rs.getLong("total_request_count"),
                        rs.getDouble("error_rate"),
                        rs.getLong("distinct_error_hosts")));
            }
        }
        return results;
    }
}
