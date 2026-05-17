package com.example.multipipelineetl.planner.mongo;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.BatchSplitter;
import com.example.multipipelineetl.connection.PostgresConnectionFactory;
import com.example.multipipelineetl.model.BatchMetadata;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.QueryType;
import com.example.multipipelineetl.persistence.MetadataRepository;
import com.example.multipipelineetl.persistence.QueryResultRepository;
import com.example.multipipelineetl.persistence.SchemaInitializer;
import com.example.multipipelineetl.pipeline.mongo.MongoQuery1Pipeline;
import com.example.multipipelineetl.pipeline.mongo.MongoQuery2Pipeline;
import com.example.multipipelineetl.pipeline.mongo.MongoQuery3Pipeline;
import com.example.multipipelineetl.planner.QueryPlanner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.List;

public class MongoQueryPlanner implements QueryPlanner {
    private Connection connection;
    private MetadataRepository metadataRepository;
    private QueryResultRepository queryResultRepository;
    private List<BatchFile> batches;
    private Path batchDirectory;
    private final MongoQuery1Pipeline query1Pipeline = new MongoQuery1Pipeline();
    private final MongoQuery2Pipeline query2Pipeline = new MongoQuery2Pipeline();
    private final MongoQuery3Pipeline query3Pipeline = new MongoQuery3Pipeline();

    public void setup(ExecutionContext context) throws Exception {
        PostgresConnectionFactory factory = new PostgresConnectionFactory();
        connection = factory.getConnection();
        new SchemaInitializer().initialize(connection);
        metadataRepository = new MetadataRepository(connection);
        queryResultRepository = new QueryResultRepository(connection);
        batchDirectory = Paths.get("target", "batches", "run_" + context.getRunId());
        batches = new BatchSplitter().split(Paths.get(context.getRequest().getDatasetPath()),
                context.getRequest().getBatchSize(), batchDirectory);
    }

    public void execute(ExecutionContext context) throws Exception {
        for (BatchFile batch : batches) {
            long start = System.currentTimeMillis();

            QueryType queryType = context.getRequest().getQueryType();
            if (queryType == QueryType.QUERY1 || queryType == QueryType.ALL) {
                query1Pipeline.execute(batch, context, queryResultRepository, connection);
            }
            if (queryType == QueryType.QUERY2 || queryType == QueryType.ALL) {
                query2Pipeline.execute(batch, context, queryResultRepository, connection);
            }
            if (queryType == QueryType.QUERY3 || queryType == QueryType.ALL) {
                query3Pipeline.execute(batch, context, queryResultRepository, connection);
            }

            long runtime = System.currentTimeMillis() - start;
            long recordCount = batch.getRecords();
            metadataRepository
                    .insertBatch(new BatchMetadata(context.getRunId(), batch.getBatchId(), recordCount, 0, runtime));
        }
    }

    public void cleanup(ExecutionContext context) throws Exception {
        if (connection != null) {
            connection.close();
        }
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
