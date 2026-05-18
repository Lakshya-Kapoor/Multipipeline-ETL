package com.example.multipipelineetl.reporting;

import com.example.multipipelineetl.connection.PostgresConnectionFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;

public class ReportService {
    private static final DecimalFormat df = new DecimalFormat("#,###");
    private static final DecimalFormat df2 = new DecimalFormat("#,###.##");

    public void printRunSummary(long runId) throws Exception {
        Connection connection = new PostgresConnectionFactory().getConnection();
        try {
            printExecutionSummary(connection, runId);
            System.out.println();
            printQuery1Results(connection, runId);
            System.out.println();
            printQuery2Results(connection, runId);
            System.out.println();
            printQuery3Results(connection, runId);
        } finally {
            connection.close();
        }
    }

    private void printExecutionSummary(Connection connection, long runId) throws Exception {
        Statement statement = connection.createStatement();
        try {
            ResultSet rs = statement.executeQuery(
                    "SELECT SUM(records_processed) as total_records, SUM(malformed_records) as total_malformed, SUM(batch_runtime_ms) as total_runtime " +
                    "FROM batch_execution_metadata WHERE run_id = " + runId);
            if (rs.next()) {
                long totalRecords = rs.getLong(1);
                long totalMalformed = rs.getLong(2);
                long totalRuntime = rs.getLong(3);
                
                System.out.println("===== EXECUTION SUMMARY: Run " + runId + " =====");
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

