package com.example.multiPipelineEtl.pipeline.query1;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.example.multiPipelineEtl.pipeline.common.QueryExecutionContext;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.mongodb.client.model.Filters;

public class MongoDbQuery1Pipeline extends AbstractQuery1Pipeline {
    private static final String PIPELINE_NAME = "MongoDBQuery1Pipeline";
    private static final Pattern LOG_PATTERN =
        Pattern.compile("^\\S+\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)]\\s+\"[^\"]*\"\\s+(\\d{3})\\s+(\\S+).*$");
    private static final DateTimeFormatter NASA_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendPattern("dd/MMM/yyyy:HH:mm:ss Z")
        .toFormatter(Locale.ENGLISH);
    private static final DateTimeFormatter ALT_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendPattern("EEE MMM dd HH:mm:ss yyyy")
        .toFormatter(Locale.ENGLISH);

    private final MongoClient mongoClient;
    private final MongoDatabase database;
    private final MongoCollection<Document> stageCollection;
    private final MongoCollection<Document> aggCollection;

    public MongoDbQuery1Pipeline(MongoClient mongoClient, String databaseName) {
        if (mongoClient == null) {
            throw new IllegalArgumentException("mongoClient cannot be null");
        }
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new IllegalArgumentException("databaseName cannot be null or empty");
        }
        this.mongoClient = mongoClient;
        this.database = mongoClient.getDatabase(databaseName);
        this.stageCollection = database.getCollection("query1_stage");
        this.aggCollection = database.getCollection("query1_agg");
    }

    @Override
    protected void extractAndParse(QueryExecutionContext context, int batchId) throws IOException, SQLException {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        if (batchId <= 0) {
            throw new IllegalArgumentException("batchId must be greater than zero");
        }

        clearBatchData(batchId);

        long startLine = (long) (batchId - 1) * context.getBatchSize();
        int linesToRead = context.getBatchSize();
        long readRecords = 0L;
        long malformedRecords = 0L;

        List<Document> documents = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(context.getInputFilePath())) {
            long currentLineIndex = 0L;
            String line;
            while ((line = reader.readLine()) != null) {
                if (currentLineIndex >= startLine) {
                    readRecords++;
                    ParsedFields parsedFields = parseLineToFields(line);
                    if (parsedFields == null) {
                        malformedRecords++;
                    } else {
                        Document doc = new Document();
                        doc.append("batch_id", batchId);
                        doc.append("log_date", parsedFields.logDate);
                        doc.append("status_code", parsedFields.statusCode);
                        doc.append("bytes_transferred", parsedFields.bytesTransferred);
                        documents.add(doc);
                    }
                    linesToRead--;
                    if (linesToRead == 0) {
                        break;
                    }
                }
                currentLineIndex++;
            }
        }

        if (!documents.isEmpty()) {
            stageCollection.insertMany(documents);
        }

        setCurrentBatchStats(readRecords, malformedRecords);
    }

    @Override
    protected void groupAndAggregate(QueryExecutionContext context, int batchId) throws SQLException {
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(new Document("$match", new Document("batch_id", batchId)));
        pipeline.add(new Document("$group", new Document()
            .append("_id", new Document()
                .append("batch_id", "$batch_id")
                .append("log_date", "$log_date")
                .append("status_code", "$status_code")
            )
            .append("request_count", new Document("$sum", 1))
            .append("total_bytes", new Document("$sum", "$bytes_transferred"))
        ));
        pipeline.add(new Document("$project", new Document()
            .append("_id", 0)
            .append("batch_id", "$_id.batch_id")
            .append("log_date", "$_id.log_date")
            .append("status_code", "$_id.status_code")
            .append("request_count", 1)
            .append("total_bytes", 1)
        ));

        List<Document> results = stageCollection.aggregate(pipeline).into(new ArrayList<>());

        if (!results.isEmpty()) {
            aggCollection.insertMany(results);
        }
    }

    @Override
    protected void load(QueryExecutionContext context, int batchId) throws SQLException {
        List<Document> results = aggCollection.find(Filters.eq("batch_id", batchId)).into(new ArrayList<>());

        String insertSql = "INSERT INTO query1_results "
            + "(execution_choice, pipeline_name, batch_id, log_date, status_code, request_count, total_bytes, executed_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        Connection postgresConnection = context.getConnection();
        try (PreparedStatement insert = postgresConnection.prepareStatement(insertSql)) {
            for (Document doc : results) {
                Number requestCount = (Number) doc.get("request_count");
                Number totalBytes = (Number) doc.get("total_bytes");
                insert.setString(1, context.getExecutionChoiceName());
                insert.setString(2, PIPELINE_NAME);
                insert.setInt(3, doc.getInteger("batch_id"));
                insert.setDate(4, Date.valueOf(doc.getString("log_date")));
                insert.setInt(5, doc.getInteger("status_code"));
                insert.setLong(6, requestCount.longValue());
                insert.setLong(7, totalBytes.longValue());
                insert.setTimestamp(8, Timestamp.from(context.getExecutionTime()));
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }


    static ParsedLogRecord parseLine(String line) {
        ParsedFields parsedFields = parseLineToFields(line);
        if (parsedFields == null) {
            return null;
        }
        return new ParsedLogRecord(parsedFields.logDate, parsedFields.statusCode, parsedFields.bytesTransferred);
    }

    private static ParsedFields parseLineToFields(String line) {
        if (line == null || line.trim().isEmpty()) {
            return null;
        }

        Matcher matcher = LOG_PATTERN.matcher(line);
        if (!matcher.matches()) {
            return null;
        }

        LocalDate localDate = parseToLocalDate(matcher.group(1));
        if (localDate == null) {
            return null;
        }

        int statusCode;
        try {
            statusCode = Integer.parseInt(matcher.group(2));
        } catch (NumberFormatException ex) {
            return null;
        }

        String bytesToken = matcher.group(3);
        long bytesTransferred;
        if ("-".equals(bytesToken)) {
            bytesTransferred = 0L;
        } else {
            try {
                bytesTransferred = Long.parseLong(bytesToken);
            } catch (NumberFormatException ex) {
                return null;
            }
        }

        return new ParsedFields(localDate.toString(), statusCode, bytesTransferred);
    }

    private static LocalDate parseToLocalDate(String timestampToken) {
        if (timestampToken == null || timestampToken.trim().isEmpty()) {
            return null;
        }

        String raw = timestampToken.trim();
        try {
            return ZonedDateTime.parse(raw, NASA_TIMESTAMP_FORMATTER).toLocalDate();
        } catch (DateTimeParseException ignored) {
        }

        try {
            return LocalDateTime.parse(raw, ALT_TIMESTAMP_FORMATTER).toLocalDate();
        } catch (DateTimeParseException ignored) {
        }
        return null;
    }

    private void clearBatchData(int batchId) {
        stageCollection.deleteMany(Filters.eq("batch_id", batchId));
        aggCollection.deleteMany(Filters.eq("batch_id", batchId));
    }

    private static final class ParsedFields {
        private final String logDate;
        private final int statusCode;
        private final long bytesTransferred;

        private ParsedFields(String logDate, int statusCode, long bytesTransferred) {
            this.logDate = logDate;
            this.statusCode = statusCode;
            this.bytesTransferred = bytesTransferred;
        }
    }
}
