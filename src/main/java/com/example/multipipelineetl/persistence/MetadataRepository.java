package com.example.multipipelineetl.persistence;

import com.example.multipipelineetl.model.BatchMetadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MetadataRepository {
    private final Connection connection;

    public MetadataRepository(Connection connection) {
        this.connection = connection;
    }

    public void insertBatch(BatchMetadata batchMetadata) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO batch_execution_metadata (run_id, query_id, batch_id, records_processed, malformed_records, batch_runtime_ms) VALUES (?, ?, ?, ?, ?, ?)");
        try {
            ps.setLong(1, batchMetadata.getRunId());
            ps.setInt(2, batchMetadata.getQueryId());
            ps.setInt(3, batchMetadata.getBatchId());
            ps.setLong(4, batchMetadata.getRecordsProcessed());
            ps.setLong(5, batchMetadata.getMalformedRecords());
            ps.setLong(6, batchMetadata.getBatchRuntimeMs());
            ps.executeUpdate();
        } finally {
            ps.close();
        }
    }
}
