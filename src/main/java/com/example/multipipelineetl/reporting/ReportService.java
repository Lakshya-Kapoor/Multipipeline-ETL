package com.example.multipipelineetl.reporting;

import com.example.multipipelineetl.connection.PostgresConnectionFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class ReportService {
    private static final DecimalFormat df = new DecimalFormat("#,###");
    private static final DecimalFormat df2 = new DecimalFormat("#,###.##");

    public void printRunSummary(long runId) throws Exception {
        Connection connection = new PostgresConnectionFactory().getConnection();
        try {
            List<Integer> queryIds = getQueryIds(connection, runId);
            if (queryIds.isEmpty()) {
                printExecutionSummary(connection, runId, null);
                System.out.println();
                printBatchExecutionStats(connection, runId, null);
            } else {
                for (int i = 0; i < queryIds.size(); i++) {
                    Integer queryId = queryIds.get(i);
                    printExecutionSummary(connection, runId, queryId);
                    System.out.println();
                    printBatchExecutionStats(connection, runId, queryId);
                    if (i < queryIds.size() - 1) {
                        System.out.println();
                    }
                }
            }

            boolean hasQuery1 = hasResults(connection, "query1_result", runId);
            boolean hasQuery2 = hasResults(connection, "query2_result", runId);
            boolean hasQuery3 = hasResults(connection, "query3_result", runId);

            if (!hasQuery1 && !hasQuery2 && !hasQuery3) {
                System.out.println();
                System.out.println("===== QUERY RESULTS =====");
                System.out.println("(No query results found for this run)");
                return;
            }

            if (hasQuery1) {
                System.out.println();
                printQuery1Results(connection, runId);
            }
            if (hasQuery2) {
                System.out.println();
                printQuery2Results(connection, runId);
            }
            if (hasQuery3) {
                System.out.println();
                printQuery3Results(connection, runId);
            }
        } finally {
            connection.close();
        }
    }

    private void printExecutionSummary(Connection connection, long runId, Integer queryId) throws Exception {
        Statement statement = connection.createStatement();
        try {
            String queryFilter = queryId == null ? "" : " AND query_id = " + queryId;
            ResultSet rs = statement.executeQuery(
                    "SELECT COUNT(DISTINCT batch_id) as total_batches, SUM(records_processed) as total_records, SUM(malformed_records) as total_malformed, SUM(batch_runtime_ms) as total_runtime " +
                    "FROM batch_execution_metadata WHERE run_id = " + runId + queryFilter);
            if (rs.next()) {
                long totalBatches = rs.getLong(1);
                long totalRecords = rs.getLong(2);
                long totalMalformed = rs.getLong(3);
                long totalRuntime = rs.getLong(4);

                String queryLabel = queryId == null ? "" : " Query " + queryId;
                System.out.println("===== EXECUTION SUMMARY: Run " + runId + queryLabel + " =====");
                System.out.println("Total Batches:            " + df.format(totalBatches));
                System.out.println("Total Records Processed:  " + df.format(totalRecords));
                System.out.println("Total Malformed Records:  " + df.format(totalMalformed));
                System.out.println("Total Runtime:            " + formatRuntime(totalRuntime));
                if (totalRecords > 0) {
                    double malformedPct = (double) totalMalformed / totalRecords * 100;
                    System.out.println("Malformed Rate:           " + df2.format(malformedPct) + "%");
                }
            }
        } finally {
            statement.close();
        }
    }

    private void printBatchExecutionStats(Connection connection, long runId, Integer queryId) throws Exception {
        Statement statement = connection.createStatement();
        try {
            String queryFilter = queryId == null ? "" : " AND query_id = " + queryId;
            ResultSet rs = statement.executeQuery(
                    "SELECT batch_id, records_processed, malformed_records, batch_runtime_ms " +
                            "FROM batch_execution_metadata WHERE run_id = " + runId + queryFilter + " " +
                            "ORDER BY batch_id");

            String queryLabel = queryId == null ? "" : " Query " + queryId;
            System.out.println("===== BATCH EXECUTION STATS" + queryLabel + " =====");
            System.out.println(String.format("%-8s | %-17s | %-18s | %-12s | %-10s",
                    "BATCH_ID", "RECORDS_PROCESSED", "MALFORMED_RECORDS", "RUNTIME", "BAD_RATE"));
            printSeparator(80);

            boolean hasResults = false;
            while (rs.next()) {
                hasResults = true;
                long records = rs.getLong(2);
                long malformed = rs.getLong(3);
                double malformedPct = records == 0 ? 0.0 : ((double) malformed / records) * 100;
                System.out.println(String.format("%-8d | %17s | %18s | %12s | %9s%%",
                        rs.getInt(1),
                        df.format(records),
                        df.format(malformed),
                        formatRuntime(rs.getLong(4)),
                        df2.format(malformedPct)));
            }

            if (!hasResults) {
                System.out.println("(No batch metadata found for this run)");
            }
        } finally {
            statement.close();
        }
    }

    private List<Integer> getQueryIds(Connection connection, long runId) throws Exception {
        Statement statement = connection.createStatement();
        try {
            ResultSet rs = statement.executeQuery(
                    "SELECT DISTINCT query_id FROM batch_execution_metadata " +
                            "WHERE run_id = " + runId + " AND query_id IS NOT NULL " +
                            "ORDER BY query_id");
            List<Integer> queryIds = new ArrayList<Integer>();
            while (rs.next()) {
                queryIds.add(rs.getInt(1));
            }
            return queryIds;
        } finally {
            statement.close();
        }
    }

    private boolean hasResults(Connection connection, String tableName, long runId) throws Exception {
        Statement statement = connection.createStatement();
        try {
            ResultSet rs = statement.executeQuery(
                    "SELECT EXISTS (SELECT 1 FROM " + tableName + " WHERE run_id = " + runId + ")");
            if (rs.next()) {
                return rs.getBoolean(1);
            }
            return false;
        } finally {
            statement.close();
        }
    }

    private void printQuery1Results(Connection connection, long runId) throws Exception {
        Statement statement = connection.createStatement();
        try {
            ResultSet rs = statement.executeQuery(
                    "SELECT log_date, status_code, SUM(request_count) as request_count, SUM(total_bytes) as total_bytes " +
                    "FROM query1_result WHERE run_id = " + runId + " " +
                    "GROUP BY log_date, status_code ORDER BY log_date, status_code LIMIT 5");
            
            System.out.println("===== QUERY 1 RESULTS (Top 5 Rows - Aggregated) =====");
            System.out.println(String.format("%-12s | %-6s | %-15s | %-15s", "LOG_DATE", "STATUS", "REQUEST_COUNT", "TOTAL_BYTES"));
            printSeparator(55);
            
            boolean hasResults = false;
            while (rs.next()) {
                hasResults = true;
                System.out.println(String.format("%-12s | %-6d | %15s | %15s",
                        rs.getDate(1),
                        rs.getInt(2),
                        df.format(rs.getLong(3)),
                        df.format(rs.getLong(4))));
            }
            
            if (!hasResults) {
                System.out.println("(No results)");
            }
        } finally {
            statement.close();
        }
    }

    private void printQuery2Results(Connection connection, long runId) throws Exception {
        Statement statement = connection.createStatement();
        try {
            ResultSet rs = statement.executeQuery(
                    "SELECT resource_path, SUM(request_count) as request_count, SUM(total_bytes) as total_bytes, SUM(distinct_host_count) as distinct_hosts " +
                    "FROM query2_result WHERE run_id = " + runId + " " +
                    "GROUP BY resource_path ORDER BY request_count DESC LIMIT 5");
            
            System.out.println("===== QUERY 2 RESULTS (Top 5 Rows - Aggregated) =====");
            System.out.println(String.format("%-30s | %-15s | %-15s | %-10s", "RESOURCE_PATH", "REQUEST_COUNT", "TOTAL_BYTES", "DISTINCT_HOSTS"));
            printSeparator(75);
            
            boolean hasResults = false;
            while (rs.next()) {
                hasResults = true;
                String path = rs.getString(1);
                if (path.length() > 30) {
                    path = path.substring(0, 27) + "...";
                }
                System.out.println(String.format("%-30s | %15s | %15s | %10s",
                        path,
                        df.format(rs.getLong(2)),
                        df.format(rs.getLong(3)),
                        df.format(rs.getLong(4))));
            }
            
            if (!hasResults) {
                System.out.println("(No results)");
            }
        } finally {
            statement.close();
        }
    }

    private void printQuery3Results(Connection connection, long runId) throws Exception {
        Statement statement = connection.createStatement();
        try {
            ResultSet rs = statement.executeQuery(
                    "SELECT log_date, log_hour, SUM(error_request_count) as error_requests, SUM(total_request_count) as total_requests, " +
                    "AVG(error_rate) as error_rate, SUM(distinct_error_hosts) as error_hosts " +
                    "FROM query3_result WHERE run_id = " + runId + " " +
                    "GROUP BY log_date, log_hour ORDER BY log_date, log_hour LIMIT 5");
            
            System.out.println("===== QUERY 3 RESULTS (Top 5 Rows - Aggregated) =====");
            System.out.println(String.format("%-12s | %-5s | %-13s | %-13s | %-10s | %-11s",
                    "LOG_DATE", "HOUR", "ERROR_REQ", "TOTAL_REQ", "ERROR_RATE", "ERROR_HOSTS"));
            printSeparator(75);
            
            boolean hasResults = false;
            while (rs.next()) {
                hasResults = true;
                System.out.println(String.format("%-12s | %5d | %13s | %13s | %10s | %11s",
                        rs.getDate(1),
                        rs.getInt(2),
                        df.format(rs.getLong(3)),
                        df.format(rs.getLong(4)),
                        df2.format(rs.getDouble(5)) + "%",
                        df.format(rs.getLong(6))));
            }
            
            if (!hasResults) {
                System.out.println("(No results)");
            }
        } finally {
            statement.close();
        }
    }

    private void printSeparator(int length) {
        for (int i = 0; i < length; i++) {
            System.out.print("-");
        }
        System.out.println();
    }

    private String formatRuntime(long milliseconds) {
        if (milliseconds < 1000) {
            return milliseconds + "ms";
        }
        long seconds = milliseconds / 1000;
        long minutes = seconds / 60;
        long secs = seconds % 60;
        if (minutes > 0) {
            return minutes + "m " + secs + "s";
        }
        return secs + "s";
    }
}
