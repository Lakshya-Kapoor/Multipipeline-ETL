package com.example.multipipelineetl.pipeline.mongo;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.NasaLogParser;
import com.example.multipipelineetl.common.ParsedLogRecord;
import com.example.multipipelineetl.connection.MongoConnectionFactory;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query1Result;
import com.example.multipipelineetl.persistence.QueryResultRepository;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MongoQuery1Pipeline {
    private static final String COLLECTION_NAME = "query1_data";
    private static final String TEMP_COLLECTION_NAME = "query1_temp";

    public void execute(BatchFile batch, ExecutionContext context, QueryResultRepository resultRepository,
            java.sql.Connection postgresConnection) throws Exception {
        List<ParsedLogRecord> parsedRows = parseLogFile(batch);

        loadDataToMongoDB(parsedRows);

        List<Query1Result> results = aggregateFromMongoDB();

        resultRepository.insertQuery1(context.getRunId(), batch.getBatchId(), "MONGO", results);

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
                    .append("statusCode", row.getStatusCode())
                    .append("bytes", row.getBytes());
            documents.add(doc);
        }

        if (!documents.isEmpty()) {
            collection.insertMany(documents);
        }
    }

    private List<Query1Result> aggregateFromMongoDB() throws Exception {
        MongoDatabase db = MongoConnectionFactory.getDatabase();
        MongoCollection<Document> collection = db.getCollection(TEMP_COLLECTION_NAME);

        Map<String, long[]> aggregated = new HashMap<>();
        for (Document doc : collection.find()) {
            String key = doc.getString("logDate") + "|" + doc.getInteger("statusCode");
            long[] stats = aggregated.get(key);
            if (stats == null) {
                stats = new long[] { 0L, 0L };
                aggregated.put(key, stats);
            }
            stats[0]++;
            stats[1] += doc.getLong("bytes");
        }

        List<Query1Result> results = new ArrayList<>();
        for (Map.Entry<String, long[]> entry : aggregated.entrySet()) {
            String[] keyParts = entry.getKey().split("\\|");
            long[] stats = entry.getValue();
            results.add(new Query1Result(Date.valueOf(keyParts[0]), Integer.parseInt(keyParts[1]), stats[0], stats[1]));
        }
        return results;
    }

    private void cleanupMongoDB() throws Exception {
        MongoDatabase db = MongoConnectionFactory.getDatabase();
        db.getCollection(TEMP_COLLECTION_NAME).drop();
    }

    public List<Query1Result> aggregate(List<ParsedLogRecord> parsedRows) {
        Map<String, long[]> aggregated = new HashMap<String, long[]>();
        for (ParsedLogRecord row : parsedRows) {
            String key = row.getLogDate().toString() + "|" + row.getStatusCode();
            long[] stats = aggregated.get(key);
            if (stats == null) {
                stats = new long[] { 0L, 0L };
                aggregated.put(key, stats);
            }
            stats[0]++;
            stats[1] += row.getBytes();
        }
        List<Query1Result> results = new ArrayList<Query1Result>();
        for (Map.Entry<String, long[]> entry : aggregated.entrySet()) {
            String[] keyParts = entry.getKey().split("\\|");
            long[] stats = entry.getValue();
            results.add(new Query1Result(Date.valueOf(keyParts[0]), Integer.parseInt(keyParts[1]), stats[0], stats[1]));
        }
        return results;
    }
}
