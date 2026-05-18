package com.example.multipipelineetl.planner.mapreduce;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.BatchSplitter;
import com.example.multipipelineetl.connection.PostgresConnectionFactory;
import com.example.multipipelineetl.model.BatchMetadata;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.QueryType;
import com.example.multipipelineetl.persistence.MetadataRepository;
import com.example.multipipelineetl.persistence.QueryResultRepository;
import com.example.multipipelineetl.pipeline.mapreduce.query1.MapReduceQuery1Pipeline;
import com.example.multipipelineetl.pipeline.mapreduce.query2.MapReduceQuery2Pipeline;
import com.example.multipipelineetl.pipeline.mapreduce.query3.MapReduceQuery3Pipeline;
import com.example.multipipelineetl.planner.QueryPlanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.List;

public class MapReduceQueryPlanner implements QueryPlanner {
    private Connection postgresConnection;
    private MetadataRepository metadataRepository;
    private QueryResultRepository queryResultRepository;
    private List<BatchFile> batches;
    private Path batchDirectory;
    private Configuration hadoopConfig;
    private FileSystem hdfs;
    
    private final MapReduceQuery1Pipeline query1Pipeline = new MapReduceQuery1Pipeline();
    private final MapReduceQuery2Pipeline query2Pipeline = new MapReduceQuery2Pipeline();
    private final MapReduceQuery3Pipeline query3Pipeline = new MapReduceQuery3Pipeline();

    @Override
    public void setup(ExecutionContext context) throws Exception {
        // Initialize Hadoop configuration
        hadoopConfig = new Configuration();
        hadoopConfig.set("fs.defaultFS", "file:///");
        hadoopConfig.set("mapreduce.framework.name", "local");
        hdfs = FileSystem.get(hadoopConfig);
        
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
            long startTime = System.currentTimeMillis();
            
            QueryType queryType = context.getRequest().getQueryType();
            
            if (queryType == QueryType.QUERY1 || queryType == QueryType.ALL) {
                query1Pipeline.execute(batch, context, queryResultRepository, hadoopConfig);
            }
            if (queryType == QueryType.QUERY2 || queryType == QueryType.ALL) {
                query2Pipeline.execute(batch, context, queryResultRepository, hadoopConfig);
            }
            if (queryType == QueryType.QUERY3 || queryType == QueryType.ALL) {
                query3Pipeline.execute(batch, context, queryResultRepository, hadoopConfig);
            }
            
            long runtime = System.currentTimeMillis() - startTime;
            long recordCount = batch.getRecords();
            metadataRepository.insertBatch(
                    new BatchMetadata(context.getRunId(), batch.getBatchId(), recordCount, 0, runtime));
        }
    }

    @Override
    public void cleanup(ExecutionContext context) throws Exception {
        // Close file systems and connections
        if (hdfs != null) {
            hdfs.close();
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
