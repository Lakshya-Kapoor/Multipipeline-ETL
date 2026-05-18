package com.example.multipipelineetl.planner.pig;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.BatchSplitter;
import com.example.multipipelineetl.connection.PostgresConnectionFactory;
import com.example.multipipelineetl.model.BatchMetadata;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.QueryType;
import com.example.multipipelineetl.persistence.MetadataRepository;
import com.example.multipipelineetl.persistence.QueryResultRepository;
import com.example.multipipelineetl.pipeline.pig.PigQuery1Pipeline;
import com.example.multipipelineetl.pipeline.pig.PigQuery2Pipeline;
import com.example.multipipelineetl.pipeline.pig.PigQuery3Pipeline;
import com.example.multipipelineetl.planner.QueryPlanner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.List;

public class PigQueryPlanner implements QueryPlanner {
    private Connection postgresConnection;
    private MetadataRepository metadataRepository;
    private QueryResultRepository queryResultRepository;
    private List<BatchFile> batches;
    private Path batchDirectory;
    
    private final PigQuery1Pipeline query1Pipeline = new PigQuery1Pipeline();
    private final PigQuery2Pipeline query2Pipeline = new PigQuery2Pipeline();
    private final PigQuery3Pipeline query3Pipeline = new PigQuery3Pipeline();

    @Override
    public void setup(ExecutionContext context) throws Exception {
        // Initialize PostgreSQL connection for metadata and results
        PostgresConnectionFactory pgFactory = new PostgresConnectionFactory();
        postgresConnection = pgFactory.getConnection();
        new com.example.multipipelineetl.persistence.SchemaInitializer().initialize(postgresConnection);
        
        // Initialize repositories
        metadataRepository = new MetadataRepository(postgresConnection);
        queryResultRepository = new QueryResultRepository(postgresConnection);
        
        // Split dataset into batches
        batchDirectory = Paths.get("target", "batches", "run_" + context.getRunId());
        batches = new BatchSplitter().split(
                Paths.get(context.getRequest().getDatasetPath()),
                context.getRequest().getBatchSize(),
                batchDirectory);
    }

    @Override
    public void execute(ExecutionContext context) throws Exception {
        for (BatchFile batch : batches) {
            QueryType queryType = context.getRequest().getQueryType();
            long recordCount = batch.getRecords();
            
            if (queryType == QueryType.QUERY1 || queryType == QueryType.ALL) {
                long startTime = System.currentTimeMillis();
                query1Pipeline.execute(batch, context, queryResultRepository, postgresConnection);
                long runtime = System.currentTimeMillis() - startTime;
                metadataRepository.insertBatch(
                        new BatchMetadata(context.getRunId(), 1, batch.getBatchId(), recordCount, 0, runtime));
            }
            if (queryType == QueryType.QUERY2 || queryType == QueryType.ALL) {
                long startTime = System.currentTimeMillis();
                query2Pipeline.execute(batch, context, queryResultRepository, postgresConnection);
                long runtime = System.currentTimeMillis() - startTime;
                metadataRepository.insertBatch(
                        new BatchMetadata(context.getRunId(), 2, batch.getBatchId(), recordCount, 0, runtime));
            }
            if (queryType == QueryType.QUERY3 || queryType == QueryType.ALL) {
                long startTime = System.currentTimeMillis();
                query3Pipeline.execute(batch, context, queryResultRepository, postgresConnection);
                long runtime = System.currentTimeMillis() - startTime;
                metadataRepository.insertBatch(
                        new BatchMetadata(context.getRunId(), 3, batch.getBatchId(), recordCount, 0, runtime));
            }
        }
    }

    @Override
    public void cleanup(ExecutionContext context) throws Exception {
        // Close PostgreSQL connection
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
        
        // Delete Pig scripts and results directories
        Path pigScriptsDir = Paths.get("target", "pig_scripts");
        if (Files.exists(pigScriptsDir)) {
            Files.walk(pigScriptsDir)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception ignored) {
                        }
                    });
        }
        
        Path pigResultsDir = Paths.get("target", "pig_results");
        if (Files.exists(pigResultsDir)) {
            Files.walk(pigResultsDir)
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
