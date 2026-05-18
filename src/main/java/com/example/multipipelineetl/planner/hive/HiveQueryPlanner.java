package com.example.multipipelineetl.planner.hive;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.HiveBatchSplitter;
import com.example.multipipelineetl.connection.HiveConnectionFactory;
import com.example.multipipelineetl.connection.PostgresConnectionFactory;
import com.example.multipipelineetl.model.BatchMetadata;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.QueryType;
import com.example.multipipelineetl.persistence.MetadataRepository;
import com.example.multipipelineetl.persistence.QueryResultRepository;
import com.example.multipipelineetl.pipeline.hive.HiveQuery1Pipeline;
import com.example.multipipelineetl.pipeline.hive.HiveQuery2Pipeline;
import com.example.multipipelineetl.pipeline.hive.HiveQuery3Pipeline;
import com.example.multipipelineetl.planner.QueryPlanner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.List;

public class HiveQueryPlanner implements QueryPlanner {
    private Connection postgresConnection;
    private Connection hiveConnection;
    private MetadataRepository metadataRepository;
    private QueryResultRepository queryResultRepository;
    private List<BatchFile> batches;
    private Path batchDirectory;
    
    private final HiveQuery1Pipeline query1Pipeline = new HiveQuery1Pipeline();
    private final HiveQuery2Pipeline query2Pipeline = new HiveQuery2Pipeline();
    private final HiveQuery3Pipeline query3Pipeline = new HiveQuery3Pipeline();

    @Override
    public void setup(ExecutionContext context) throws Exception {
        // Initialize PostgreSQL connection for metadata and results
        PostgresConnectionFactory pgFactory = new PostgresConnectionFactory();
        postgresConnection = pgFactory.getConnection();
        new com.example.multipipelineetl.persistence.SchemaInitializer().initialize(postgresConnection);
        
        // Initialize Hive connection
        HiveConnectionFactory hiveFactory = new HiveConnectionFactory();
        hiveConnection = hiveFactory.getConnection();
        
        // Initialize repositories
        metadataRepository = new MetadataRepository(postgresConnection);
        queryResultRepository = new QueryResultRepository(postgresConnection);
        
        // Split dataset into batches
        batchDirectory = Paths.get("target", "batches", "run_" + context.getRunId());
        batches = new HiveBatchSplitter().split(
                Paths.get(context.getRequest().getDatasetPath()),
                context.getRequest().getBatchSize(),
                batchDirectory);
    }

    @Override
    public void execute(ExecutionContext context) throws Exception {
        for (BatchFile batch : batches) {
            QueryType queryType = context.getRequest().getQueryType();
            long recordCount = batch.getRecords();
            
            // Use HDFS path if available, otherwise use local absolute path
            String batchPath = batch.getHdfsPath() != null ? batch.getHdfsPath() : batch.getPath().toAbsolutePath().toString();
            
            if (queryType == QueryType.QUERY1 || queryType == QueryType.ALL) {
                long startTime = System.currentTimeMillis();
                query1Pipeline.execute(batch, batchPath, context, queryResultRepository, hiveConnection);
                long runtime = System.currentTimeMillis() - startTime;
                metadataRepository.insertBatch(
                        new BatchMetadata(context.getRunId(), 1, batch.getBatchId(), recordCount, 0, runtime));
            }
            if (queryType == QueryType.QUERY2 || queryType == QueryType.ALL) {
                long startTime = System.currentTimeMillis();
                query2Pipeline.execute(batch, batchPath, context, queryResultRepository, hiveConnection);
                long runtime = System.currentTimeMillis() - startTime;
                metadataRepository.insertBatch(
                        new BatchMetadata(context.getRunId(), 2, batch.getBatchId(), recordCount, 0, runtime));
            }
            if (queryType == QueryType.QUERY3 || queryType == QueryType.ALL) {
                long startTime = System.currentTimeMillis();
                query3Pipeline.execute(batch, batchPath, context, queryResultRepository, hiveConnection);
                long runtime = System.currentTimeMillis() - startTime;
                metadataRepository.insertBatch(
                        new BatchMetadata(context.getRunId(), 3, batch.getBatchId(), recordCount, 0, runtime));
            }
        }
    }

    @Override
    public void cleanup(ExecutionContext context) throws Exception {
        // Close connections
        if (hiveConnection != null) {
            hiveConnection.close();
        }
        if (postgresConnection != null) {
            postgresConnection.close();
        }
        
        // Delete batch files
        if (batchDirectory != null && Files.exists(batchDirectory)) {
            Files.walk(batchDirectory)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception ignored) {
                        }
                    });
        }
    }
}
