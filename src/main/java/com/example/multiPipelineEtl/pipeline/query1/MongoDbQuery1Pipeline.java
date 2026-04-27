package com.example.multiPipelineEtl.pipeline.query1;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.example.multiPipelineEtl.pipeline.common.QueryExecutionContext;

public class MongoDbQuery1Pipeline extends AbstractQuery1Pipeline {
    private static final String PIPELINE_NAME = "MongoDBJDBCQuery1Pipeline";
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

    private final Connection mongoConnection;
    private boolean stageTableInitialized = false;

    public MongoDbQuery1Pipeline(Connection mongoConnection) {
        if (mongoConnection == null) {
            throw new IllegalArgumentException("mongoConnection cannot be null");
        }
        this.mongoConnection = mongoConnection;
    }

    @Override
    protected void extractAndParse(QueryExecutionContext context, int batchId) throws IOException, SQLException {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        if (batchId <= 0) {
            throw new IllegalArgumentException("batchId must be greater than zero");
        }

        ensureMongoWorkingTablesInitialized();
        clearBatchData(batchId);

        long startLine = (long) (batchId - 1) * context.getBatchSize();
        int linesToRead = context.getBatchSize();
        long readRecords = 0L;
        long malformedRecords = 0L;

        String insertSql = "INSERT INTO query1_stage (batch_id, log_date, status_code, bytes_transferred) VALUES (?, ?, ?, ?)";
        try (BufferedReader reader = Files.newBufferedReader(context.getInputFilePath());
             PreparedStatement insertStatement = mongoConnection.prepareStatement(insertSql)) {
            long currentLineIndex = 0L;
            String line;
            while ((line = reader.readLine()) != null) {
                if (currentLineIndex >= startLine) {
                    readRecords++;
                    ParsedFields parsedFields = parseLineToFields(line);
                    if (parsedFields == null) {
                        malformedRecords++;
                    } else {
                        insertStatement.setInt(1, batchId);
                        insertStatement.setString(2, parsedFields.logDate);
                        insertStatement.setInt(3, parsedFields.statusCode);
                        insertStatement.setLong(4, parsedFields.bytesTransferred);
                        insertStatement.addBatch();
                    }
                    linesToRead--;
                    if (linesToRead == 0) {
                        break;
                    }
                }
                currentLineIndex++;
            }
            insertStatement.executeBatch();
        }

        setCurrentBatchStats(readRecords, malformedRecords);
    }

    @Override
    protected void groupAndAggregate(QueryExecutionContext context, int batchId) throws SQLException {
        String aggregateSql =
            "INSERT INTO query1_agg (batch_id, log_date, status_code, request_count, total_bytes) "
                + "SELECT batch_id, log_date, status_code, COUNT(*), COALESCE(SUM(bytes_transferred), 0) "
                + "FROM query1_stage WHERE batch_id = ? GROUP BY batch_id, log_date, status_code";

        try (PreparedStatement statement = mongoConnection.prepareStatement(aggregateSql)) {
            statement.setInt(1, batchId);
            statement.executeUpdate();
        }
    }

    @Override
    protected void load(QueryExecutionContext context, int batchId) throws SQLException {
        String selectSql =
            "SELECT log_date, status_code, request_count, total_bytes FROM query1_agg WHERE batch_id = ?";
        String insertSql = "INSERT INTO query1_results "
            + "(execution_choice, pipeline_name, batch_id, log_date, status_code, request_count, total_bytes, executed_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement select = mongoConnection.prepareStatement(selectSql);
             PreparedStatement insert = context.getConnection().prepareStatement(insertSql)) {
            select.setInt(1, batchId);
            try (ResultSet resultSet = select.executeQuery()) {
                while (resultSet.next()) {
                    insert.setString(1, context.getExecutionChoiceName());
                    insert.setString(2, PIPELINE_NAME);
                    insert.setInt(3, batchId);
                    insert.setDate(4, Date.valueOf(resultSet.getString("log_date")));
                    insert.setInt(5, resultSet.getInt("status_code"));
                    insert.setLong(6, resultSet.getLong("request_count"));
                    insert.setLong(7, resultSet.getLong("total_bytes"));
                    insert.setTimestamp(8, Timestamp.from(context.getExecutionTime()));
                    insert.addBatch();
                }
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

    private void ensureMongoWorkingTablesInitialized() throws SQLException {
        if (stageTableInitialized) {
            return;
        }

        String createStageSql = "CREATE TABLE IF NOT EXISTS query1_stage ("
            + "batch_id INTEGER, "
            + "log_date VARCHAR(20), "
            + "status_code INTEGER, "
            + "bytes_transferred BIGINT"
            + ")";
        String createAggregateSql = "CREATE TABLE IF NOT EXISTS query1_agg ("
            + "batch_id INTEGER, "
            + "log_date VARCHAR(20), "
            + "status_code INTEGER, "
            + "request_count BIGINT, "
            + "total_bytes BIGINT"
            + ")";

        try (Statement statement = mongoConnection.createStatement()) {
            statement.executeUpdate(createStageSql);
            statement.executeUpdate(createAggregateSql);
        }
        stageTableInitialized = true;
    }

    private void clearBatchData(int batchId) throws SQLException {
        try (PreparedStatement deleteStage = mongoConnection.prepareStatement(
            "DELETE FROM query1_stage WHERE batch_id = ?"
        );
             PreparedStatement deleteAgg = mongoConnection.prepareStatement(
                 "DELETE FROM query1_agg WHERE batch_id = ?"
             )) {
            deleteStage.setInt(1, batchId);
            deleteStage.executeUpdate();
            deleteAgg.setInt(1, batchId);
            deleteAgg.executeUpdate();
        }
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
