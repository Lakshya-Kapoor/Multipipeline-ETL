package com.example.multipipelineetl.persistence;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SchemaInitializer {
    public void initialize(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        try {
            statement.execute("CREATE TABLE IF NOT EXISTS batch_execution_metadata (" +
                    "id BIGSERIAL PRIMARY KEY," +
                    "run_id BIGINT," +
                    "batch_id INT," +
                    "records_processed BIGINT," +
                    "malformed_records BIGINT," +
                    "batch_runtime_ms BIGINT," +
                    "batch_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                    "batch_end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            statement.execute("CREATE TABLE IF NOT EXISTS query1_result (" +
                    "id BIGSERIAL PRIMARY KEY," +
                    "run_id BIGINT," +
                    "batch_id INT," +
                    "pipeline_name VARCHAR(50)," +
                    "log_date DATE," +
                    "status_code INT," +
                    "request_count BIGINT," +
                    "total_bytes BIGINT)");
            statement.execute("CREATE TABLE IF NOT EXISTS query2_result (" +
                    "id BIGSERIAL PRIMARY KEY," +
                    "run_id BIGINT," +
                    "batch_id INT," +
                    "pipeline_name VARCHAR(50)," +
                    "resource_path TEXT," +
                    "request_count BIGINT," +
                    "total_bytes BIGINT," +
                    "distinct_host_count BIGINT)");
            statement.execute("CREATE TABLE IF NOT EXISTS query3_result (" +
                    "id BIGSERIAL PRIMARY KEY," +
                    "run_id BIGINT," +
                    "batch_id INT," +
                    "pipeline_name VARCHAR(50)," +
                    "log_date DATE," +
                    "log_hour INT," +
                    "error_request_count BIGINT," +
                    "total_request_count BIGINT," +
                    "error_rate DOUBLE PRECISION," +
                    "distinct_error_hosts BIGINT)");
        } finally {
            statement.close();
        }
    }
}

