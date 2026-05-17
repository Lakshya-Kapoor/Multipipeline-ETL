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

        Map<String, long[]> counters = new HashMap<>();
        Map<String, Set<String>> errorHosts = new HashMap<>();

        for (Document doc : collection.find()) {
            String key = doc.getString("logDate") + "|" + doc.getInteger("logHour");
            long[] stats = counters.get(key);
            if (stats == null) {
                stats = new long[] { 0L, 0L };
                counters.put(key, stats);
            }
            stats[1]++;

            int statusCode = doc.getInteger("statusCode");
            if (statusCode >= 400 && statusCode <= 599) {
                stats[0]++;
                Set<String> hosts = errorHosts.get(key);
                if (hosts == null) {
                    hosts = new HashSet<>();
                    errorHosts.put(key, hosts);
                }
                hosts.add(doc.getString("host"));
            }
        }

        List<Query3Result> results = new ArrayList<>();
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
