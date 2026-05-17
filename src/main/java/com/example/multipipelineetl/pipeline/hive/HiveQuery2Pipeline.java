package com.example.multipipelineetl.pipeline.hive;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.HiveTableManager;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query2Result;
import com.example.multipipelineetl.persistence.QueryResultRepository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class HiveQuery2Pipeline {
    private static final String TABLE_PREFIX = "hive_query2_batch_";
    private static final String VIEW_NAME = "hive_query2_transformed";

    public void execute(BatchFile batch, ExecutionContext context, QueryResultRepository resultRepository,
                        Connection hiveConnection) throws Exception {
        String tableName = TABLE_PREFIX + batch.getBatchId();
        String batchFilePath = batch.getPath().toString();

        try {
            // Create external table
            HiveTableManager.createExternalTable(hiveConnection, tableName, batchFilePath);

            // Create transformation view
            HiveTableManager.createTransformationView(hiveConnection, VIEW_NAME, tableName);

            // Execute aggregation query
            List<Query2Result> results = executeAggregation(hiveConnection);

            // Load results to PostgreSQL
            resultRepository.insertQuery2(context.getRunId(), batch.getBatchId(), "HIVE", results);

        } finally {
            // Cleanup
            HiveTableManager.dropTable(hiveConnection, tableName);
        }
    }

    private List<Query2Result> executeAggregation(Connection hiveConnection) throws Exception {
        String aggregationQuery = String.format(
                "SELECT\n" +
                "  resource_path,\n" +
                "  COUNT(*) AS request_count,\n" +
                "  SUM(bytes) AS total_bytes,\n" +
                "  COUNT(DISTINCT host) AS distinct_host_count\n" +
                "FROM %s\n" +
                "GROUP BY resource_path\n" +
                "ORDER BY request_count DESC\n" +
                "LIMIT 20",
                VIEW_NAME);

        List<Query2Result> results = new ArrayList<>();
        try (Statement stmt = hiveConnection.createStatement()) {
            ResultSet rs = stmt.executeQuery(aggregationQuery);
            while (rs.next()) {
                results.add(new Query2Result(
                        rs.getString("resource_path"),
                        rs.getLong("request_count"),
                        rs.getLong("total_bytes"),
                        rs.getLong("distinct_host_count")));
            }
        }
        return results;
    }
}
