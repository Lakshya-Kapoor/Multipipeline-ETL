package com.example.multipipelineetl.pipeline.mongo;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.NasaLogParser;
import com.example.multipipelineetl.common.ParsedLogRecord;
import com.example.multipipelineetl.connection.MongoConnectionFactory;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query2Result;
import com.example.multipipelineetl.persistence.QueryResultRepository;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MongoQuery2Pipeline {
    private static final String TEMP_COLLECTION_NAME = "query2_temp";

    public void execute(BatchFile batch, ExecutionContext context, QueryResultRepository resultRepository,
            java.sql.Connection postgresConnection) throws Exception {
        List<ParsedLogRecord> parsedRows = parseLogFile(batch);

        loadDataToMongoDB(parsedRows);

        List<Query2Result> results = aggregateFromMongoDB();

        resultRepository.insertQuery2(context.getRunId(), batch.getBatchId(), "MONGO", results);

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
                    .append("resourcePath", row.getResourcePath())
                    .append("bytes", row.getBytes())
                    .append("host", row.getHost());
            documents.add(doc);
        }

        if (!documents.isEmpty()) {
            collection.insertMany(documents);
        }
    }

    private List<Query2Result> aggregateFromMongoDB() throws Exception {
        MongoDatabase db = MongoConnectionFactory.getDatabase();
        MongoCollection<Document> collection = db.getCollection(TEMP_COLLECTION_NAME);

        Map<String, long[]> metrics = new HashMap<>();
        Map<String, Set<String>> hosts = new HashMap<>();

        for (Document doc : collection.find()) {
            String path = doc.getString("resourcePath");
            long[] stats = metrics.get(path);
            if (stats == null) {
                stats = new long[] { 0L, 0L };
                metrics.put(path, stats);
            }
            stats[0]++;
            stats[1] += doc.getLong("bytes");

            Set<String> hostSet = hosts.get(path);
            if (hostSet == null) {
                hostSet = new HashSet<>();
                hosts.put(path, hostSet);
            }
            hostSet.add(doc.getString("host"));
        }

        List<Query2Result> all = new ArrayList<>();
        for (Map.Entry<String, long[]> entry : metrics.entrySet()) {
            String resource = entry.getKey();
            long[] stats = entry.getValue();
            all.add(new Query2Result(resource, stats[0], stats[1], hosts.get(resource).size()));
        }

        all.sort(new Comparator<Query2Result>() {
            public int compare(Query2Result left, Query2Result right) {
                return Long.compare(right.getRequestCount(), left.getRequestCount());
            }
        });

        return all.subList(0, Math.min(20, all.size()));
    }

    private void cleanupMongoDB() throws Exception {
        MongoDatabase db = MongoConnectionFactory.getDatabase();
        db.getCollection(TEMP_COLLECTION_NAME).drop();
    }

    public List<Query2Result> aggregate(List<ParsedLogRecord> parsedRows) {
        Map<String, long[]> metrics = new HashMap<String, long[]>();
        Map<String, Set<String>> hosts = new HashMap<String, Set<String>>();
        for (ParsedLogRecord row : parsedRows) {
            String path = row.getResourcePath();
            long[] stats = metrics.get(path);
            if (stats == null) {
                stats = new long[] { 0L, 0L };
                metrics.put(path, stats);
            }
            stats[0]++;
            stats[1] += row.getBytes();
            Set<String> hostSet = hosts.get(path);
            if (hostSet == null) {
                hostSet = new HashSet<String>();
                hosts.put(path, hostSet);
            }
            hostSet.add(row.getHost());
        }
        List<Query2Result> all = new ArrayList<Query2Result>();
        for (Map.Entry<String, long[]> entry : metrics.entrySet()) {
            String resource = entry.getKey();
            long[] stats = entry.getValue();
            all.add(new Query2Result(resource, stats[0], stats[1], hosts.get(resource).size()));
        }
        all.sort(new Comparator<Query2Result>() {
            public int compare(Query2Result left, Query2Result right) {
                return Long.compare(right.getRequestCount(), left.getRequestCount());
            }
        });
        return all.subList(0, Math.min(20, all.size()));
    }
}
