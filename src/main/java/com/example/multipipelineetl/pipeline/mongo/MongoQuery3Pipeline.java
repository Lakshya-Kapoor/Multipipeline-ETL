package com.example.multipipelineetl.pipeline.mongo;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.NasaLogParser;
import com.example.multipipelineetl.common.ParsedLogRecord;
import com.example.multipipelineetl.connection.MongoConnectionFactory;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query3Result;
import com.example.multipipelineetl.persistence.QueryResultRepository;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MongoQuery3Pipeline {
    private static final String TEMP_COLLECTION_NAME = "query3_temp";

    public void execute(BatchFile batch, ExecutionContext context, QueryResultRepository resultRepository,
            java.sql.Connection postgresConnection) throws Exception {
        List<ParsedLogRecord> parsedRows = parseLogFile(batch);

        loadDataToMongoDB(parsedRows);

        List<Query3Result> results = aggregateFromMongoDB();

        resultRepository.insertQuery3(context.getRunId(), batch.getBatchId(), "MONGO", results);

        cleanupMongoDB();
    }

    private List<ParsedLogRecord> parseLogFile(BatchFile batch) throws Exception {
        List<String> lines = Files.readAllLines(batch.getPath(), StandardCharsets.UTF_8);
        List<ParsedLogRecord> parsedRows = new ArrayList<>();
        for (String line : lines) {
            Optional<ParsedLogRecord> parsed = NasaLogParser.parse(line);
            if (parsed.isPresent()) {
                parsedRows.add(parsed.get());
            }
        }
        return parsedRows;
    }

    private void loadDataToMongoDB(List<ParsedLogRecord> parsedRows) throws Exception {
        MongoDatabase db = MongoConnectionFactory.getDatabase();
        MongoCollection<Document> collection = db.getCollection(TEMP_COLLECTION_NAME);

        List<Document> documents = new ArrayList<>();
        for (ParsedLogRecord row : parsedRows) {
            Document doc = new Document()
                    .append("logDate", row.getLogDate().toString())
                    .append("logHour", row.getLogHour())
                    .append("statusCode", row.getStatusCode())
                    .append("host", row.getHost());
            documents.add(doc);
        }

        if (!documents.isEmpty()) {
            collection.insertMany(documents);
        }
    }

    private List<Query3Result> aggregateFromMongoDB() throws Exception {
        MongoDatabase db = MongoConnectionFactory.getDatabase();
        MongoCollection<Document> collection = db.getCollection(TEMP_COLLECTION_NAME);

        List<Document> pipeline = new ArrayList<>();
        
        // Group by logDate and logHour, aggregating request counts and error host information
        pipeline.add(new Document("$group", new Document()
                .append("_id", new Document()
                        .append("logDate", "$logDate")
                        .append("logHour", "$logHour"))
                .append("totalRequestCount", new Document("$sum", 1))
                .append("errorRequestCount", new Document("$sum", new Document("$cond", Arrays.asList(
                        new Document("$and", new ArrayList<Object>() {{
                            add(new Document("$gte", Arrays.asList("$statusCode", 400)));
                            add(new Document("$lte", Arrays.asList("$statusCode", 599)));
                        }}),
                        1,
                        0))))
                .append("errorHosts", new Document("$addToSet", new Document("$cond", Arrays.asList(
                        new Document("$and", new ArrayList<Object>() {{
                            add(new Document("$gte", Arrays.asList("$statusCode", 400)));
                            add(new Document("$lte", Arrays.asList("$statusCode", 599)));
                        }}),
                        "$host",
                        null))))));
        
        // Project the final output format
        pipeline.add(new Document("$project", new Document()
                .append("_id", 0)
                .append("logDate", "$_id.logDate")
                .append("logHour", "$_id.logHour")
                .append("errorRequestCount", 1)
                .append("totalRequestCount", 1)
                .append("distinctErrorHosts", new Document("$size", 
                        new Document("$filter", new Document()
                                .append("input", "$errorHosts")
                                .append("as", "host")
                                .append("cond", new Document("$ne", Arrays.asList("$$host", null))))))
                .append("errorRate", new Document("$cond", Arrays.asList(
                        new Document("$eq", Arrays.asList("$totalRequestCount", 0)),
                        0.0,
                        new Document("$divide", Arrays.asList("$errorRequestCount", "$totalRequestCount")))))));

        List<Document> aggregatedDocs = collection.aggregate(pipeline).into(new ArrayList<>());

        List<Query3Result> results = new ArrayList<>();
        for (Document doc : aggregatedDocs) {
            results.add(new Query3Result(
                    Date.valueOf(doc.getString("logDate")),
                    doc.getInteger("logHour"),
                    doc.getLong("errorRequestCount"),
                    doc.getLong("totalRequestCount"),
                    doc.getDouble("errorRate"),
                    doc.getLong("distinctErrorHosts")));
        }
        return results;
    }

    private void cleanupMongoDB() throws Exception {
        MongoDatabase db = MongoConnectionFactory.getDatabase();
        db.getCollection(TEMP_COLLECTION_NAME).drop();
    }

    public List<Query3Result> aggregate(List<ParsedLogRecord> parsedRows) {
        Map<String, long[]> counters = new HashMap<String, long[]>();
        Map<String, Set<String>> errorHosts = new HashMap<String, Set<String>>();

        for (ParsedLogRecord row : parsedRows) {
            String key = row.getLogDate().toString() + "|" + row.getLogHour();
            long[] stats = counters.get(key);
            if (stats == null) {
                stats = new long[] { 0L, 0L };
                counters.put(key, stats);
            }
            stats[1]++;
            if (row.getStatusCode() >= 400 && row.getStatusCode() <= 599) {
                stats[0]++;
                Set<String> hosts = errorHosts.get(key);
                if (hosts == null) {
                    hosts = new HashSet<String>();
                    errorHosts.put(key, hosts);
                }
                hosts.add(row.getHost());
            }
        }

        List<Query3Result> results = new ArrayList<Query3Result>();
        for (Map.Entry<String, long[]> entry : counters.entrySet()) {
            String[] keyParts = entry.getKey().split("\\|");
            long[] stats = entry.getValue();
            long errorCount = stats[0];
            long totalCount = stats[1];
            double rate = totalCount == 0 ? 0.0 : ((double) errorCount / (double) totalCount);
            Set<String> hosts = errorHosts.get(entry.getKey());
            long distinctErrorHosts = hosts == null ? 0L : hosts.size();
            results.add(new Query3Result(Date.valueOf(keyParts[0]), Integer.parseInt(keyParts[1]), errorCount,
                    totalCount, rate, distinctErrorHosts));
        }
        return results;
    }
}
