package com.example.multiPipelineEtl.reporting;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ReportingModule {
    private final PrintStream out;

    public ReportingModule(PrintStream out) {
        if (out == null) {
            throw new IllegalArgumentException("out cannot be null");
        }
        this.out = out;
    }

    public void printQuery1Results(Connection connection) throws SQLException {
        String sql = "SELECT execution_choice, pipeline_name, batch_id, log_date, status_code, "
            + "request_count, total_bytes, executed_at "
            + "FROM query1_results ORDER BY executed_at DESC, batch_id ASC, log_date ASC, status_code ASC";
        out.println("=== Query 1 Results: Daily Traffic Summary ===");
        executeAndPrint(connection, sql);
    }

    public void printQuery2Results(Connection connection) throws SQLException {
        String sql = "SELECT execution_choice, pipeline_name, batch_id, resource_path, "
            + "request_count, total_bytes, distinct_host_count, executed_at "
            + "FROM query2_results ORDER BY executed_at DESC, batch_id ASC, request_count DESC";
        out.println("=== Query 2 Results: Top Requested Resources ===");
        executeAndPrint(connection, sql);
    }

    public void printQuery3Results(Connection connection) throws SQLException {
        String sql = "SELECT execution_choice, pipeline_name, batch_id, log_date, log_hour, "
            + "error_request_count, total_request_count, error_rate, distinct_error_hosts, executed_at "
            + "FROM query3_results ORDER BY executed_at DESC, batch_id ASC, log_date ASC, log_hour ASC";
        out.println("=== Query 3 Results: Hourly Error Analysis ===");
        executeAndPrint(connection, sql);
    }

    public void printExecutionMetadata(Connection connection) throws SQLException {
        String sql = "SELECT execution_choice, pipeline_name, query_number, query_name, batch_id, batch_size, "
            + "records_processed, malformed_records, runtime_millis, average_batch_size, executed_at "
            + "FROM execution_metadata ORDER BY executed_at DESC, query_number ASC, batch_id ASC";
        out.println("=== Execution Metadata ===");
        executeAndPrint(connection, sql);
    }

    public void printAll(Connection connection) throws SQLException {
        printQuery1Results(connection);
        printQuery2Results(connection);
        printQuery3Results(connection);
        printExecutionMetadata(connection);
    }

    private void executeAndPrint(Connection connection, String sql) throws SQLException {
        if (connection == null) {
            throw new IllegalArgumentException("connection cannot be null");
        }

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            int columnCount = resultSet.getMetaData().getColumnCount();
            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                out.print(resultSet.getMetaData().getColumnName(columnIndex));
                if (columnIndex < columnCount) {
                    out.print(" | ");
                }
            }
            out.println();

            while (resultSet.next()) {
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    Object value = resultSet.getObject(columnIndex);
                    out.print(value == null ? "NULL" : value.toString());
                    if (columnIndex < columnCount) {
                        out.print(" | ");
                    }
                }
                out.println();
            }
            out.println();
        }
    }
}
