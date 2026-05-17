package com.example.multipipelineetl.reporting;

import com.example.multipipelineetl.connection.PostgresConnectionFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class ReportService {
    public void printRunSummary(long runId) throws Exception {
        Connection connection = new PostgresConnectionFactory().getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet rs = statement.executeQuery("SELECT batch_id, records_processed, malformed_records, batch_runtime_ms FROM batch_execution_metadata WHERE run_id = " + runId + " ORDER BY batch_id");
                System.out.println("Run " + runId + " batch summary:");
                while (rs.next()) {
                    System.out.println("Batch " + rs.getInt(1)
                            + " processed=" + rs.getLong(2)
                            + " malformed=" + rs.getLong(3)
                            + " runtimeMs=" + rs.getLong(4));
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }
}

