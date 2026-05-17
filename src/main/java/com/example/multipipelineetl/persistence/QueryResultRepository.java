package com.example.multipipelineetl.persistence;

import com.example.multipipelineetl.model.Query1Result;
import com.example.multipipelineetl.model.Query2Result;
import com.example.multipipelineetl.model.Query3Result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class QueryResultRepository {
    private final Connection connection;

    public QueryResultRepository(Connection connection) {
        this.connection = connection;
    }

    public void insertQuery1(long runId, int batchId, String pipelineName, List<Query1Result> rows) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO query1_result (run_id, batch_id, pipeline_name, log_date, status_code, request_count, total_bytes) VALUES (?, ?, ?, ?, ?, ?, ?)");
        try {
            for (Query1Result row : rows) {
                ps.setLong(1, runId);
                ps.setInt(2, batchId);
                ps.setString(3, pipelineName);
                ps.setDate(4, row.getLogDate());
                ps.setInt(5, row.getStatusCode());
                ps.setLong(6, row.getRequestCount());
                ps.setLong(7, row.getTotalBytes());
                ps.addBatch();
            }
            ps.executeBatch();
        } finally {
            ps.close();
        }
    }

    public void insertQuery2(long runId, int batchId, String pipelineName, List<Query2Result> rows) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO query2_result (run_id, batch_id, pipeline_name, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?, ?, ?, ?, ?, ?, ?)");
        try {
            for (Query2Result row : rows) {
                ps.setLong(1, runId);
                ps.setInt(2, batchId);
                ps.setString(3, pipelineName);
                ps.setString(4, row.getResourcePath());
                ps.setLong(5, row.getRequestCount());
                ps.setLong(6, row.getTotalBytes());
                ps.setLong(7, row.getDistinctHostCount());
                ps.addBatch();
            }
            ps.executeBatch();
        } finally {
            ps.close();
        }
    }

    public void insertQuery3(long runId, int batchId, String pipelineName, List<Query3Result> rows) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO query3_result (run_id, batch_id, pipeline_name, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        try {
            for (Query3Result row : rows) {
                ps.setLong(1, runId);
                ps.setInt(2, batchId);
                ps.setString(3, pipelineName);
                ps.setDate(4, row.getLogDate());
                ps.setInt(5, row.getLogHour());
                ps.setLong(6, row.getErrorRequestCount());
                ps.setLong(7, row.getTotalRequestCount());
                ps.setDouble(8, row.getErrorRate());
                ps.setLong(9, row.getDistinctErrorHosts());
                ps.addBatch();
            }
            ps.executeBatch();
        } finally {
            ps.close();
        }
    }
}

