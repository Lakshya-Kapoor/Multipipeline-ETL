package com.example.multipipelineetl.common;

import java.nio.file.Files;
import java.nio.file.Path;

public class PigScriptGenerator {

    public static String generateQuery1Script(String batchFilePath, String outputPath) {
        return String.format(
                "-- Query 1: Daily Traffic Summary\n" +
                "logs = LOAD '%s' AS (line:chararray);\n" +
                "\n" +
                "-- Parse log lines\n" +
                "parsed = FOREACH logs GENERATE\n" +
                "  REGEX_EXTRACT(line, '^(\\\\S+)\\\\s+\\\\S+\\\\s+\\\\S+\\\\s+\\\\[(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2}):\\\\d{2}:\\\\d{2}', 1) AS host,\n" +
                "  CONCAT(REGEX_EXTRACT(line, '^(\\\\S+)\\\\s+\\\\S+\\\\s+\\\\S+\\\\s+\\\\[(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2}):', 3), '-',\n" +
                "         REGEX_EXTRACT(line, '^(\\\\S+)\\\\s+\\\\S+\\\\s+\\\\S+\\\\s+\\\\[(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2}):', 2), '-',\n" +
                "         REGEX_EXTRACT(line, '^(\\\\S+)\\\\s+\\\\S+\\\\s+\\\\S+\\\\s+\\\\[(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2}):', 4)) AS log_date,\n" +
                "  REGEX_EXTRACT(line, '\\\\s+(\\\\d{3})\\\\s+', 0) AS status_code,\n" +
                "  REGEX_EXTRACT(line, '\\\\s+(\\\\S+)$', 0) AS bytes_str;\n" +
                "\n" +
                "-- Filter out malformed records\n" +
                "filtered = FILTER parsed BY host IS NOT NULL AND status_code IS NOT NULL;\n" +
                "\n" +
                "-- Convert bytes (handle '-' as 0)\n" +
                "with_bytes = FOREACH filtered GENERATE\n" +
                "  log_date,\n" +
                "  (int)status_code AS status_code,\n" +
                "  (CASE WHEN bytes_str == '-' THEN 0L ELSE (long)bytes_str END) AS bytes;\n" +
                "\n" +
                "-- Group by log_date and status_code\n" +
                "grouped = GROUP with_bytes BY (log_date, status_code);\n" +
                "\n" +
                "-- Aggregate\n" +
                "result = FOREACH grouped GENERATE\n" +
                "  group.log_date AS log_date,\n" +
                "  group.status_code AS status_code,\n" +
                "  COUNT(with_bytes) AS request_count,\n" +
                "  SUM(with_bytes.bytes) AS total_bytes;\n" +
                "\n" +
                "STORE result INTO '%s' USING PigStorage(',');",
                batchFilePath, outputPath);
    }

    public static String generateQuery2Script(String batchFilePath, String outputPath) {
        return String.format(
                "-- Query 2: Top 20 Resources\n" +
                "logs = LOAD '%s' AS (line:chararray);\n" +
                "\n" +
                "-- Parse log lines\n" +
                "parsed = FOREACH logs GENERATE\n" +
                "  REGEX_EXTRACT(line, '^(\\\\S+)\\\\s+\\\\S+\\\\s+\\\\S+\\\\s+\\\\[(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2}):\\\\d{2}:\\\\d{2}', 1) AS host,\n" +
                "  REGEX_EXTRACT(line, '\\\\\"\\\\S+\\\\s+([^\\\\s]+)\\\\s+', 1) AS resource_path,\n" +
                "  REGEX_EXTRACT(line, '\\\\s+(\\\\S+)$', 0) AS bytes_str;\n" +
                "\n" +
                "-- Filter out malformed records\n" +
                "filtered = FILTER parsed BY resource_path IS NOT NULL AND host IS NOT NULL;\n" +
                "\n" +
                "-- Convert bytes\n" +
                "with_bytes = FOREACH filtered GENERATE\n" +
                "  host,\n" +
                "  resource_path,\n" +
                "  (CASE WHEN bytes_str == '-' THEN 0L ELSE (long)bytes_str END) AS bytes;\n" +
                "\n" +
                "-- Group by resource_path\n" +
                "grouped = GROUP with_bytes BY resource_path;\n" +
                "\n" +
                "-- Aggregate\n" +
                "result_temp = FOREACH grouped GENERATE\n" +
                "  group AS resource_path,\n" +
                "  COUNT(with_bytes) AS request_count,\n" +
                "  SUM(with_bytes.bytes) AS total_bytes,\n" +
                "  COUNT(DISTINCT with_bytes.host) AS distinct_host_count;\n" +
                "\n" +
                "-- Sort by request_count DESC and LIMIT 20\n" +
                "sorted = ORDER result_temp BY request_count DESC;\n" +
                "result = LIMIT sorted 20;\n" +
                "\n" +
                "STORE result INTO '%s' USING PigStorage(',');",
                batchFilePath, outputPath);
    }

    public static String generateQuery3Script(String batchFilePath, String outputPath) {
        return String.format(
                "-- Query 3: Hourly Error Analysis\n" +
                "logs = LOAD '%s' AS (line:chararray);\n" +
                "\n" +
                "-- Parse log lines\n" +
                "parsed = FOREACH logs GENERATE\n" +
                "  CONCAT(REGEX_EXTRACT(line, '^(\\\\S+)\\\\s+\\\\S+\\\\s+\\\\S+\\\\s+\\\\[(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2}):', 3), '-',\n" +
                "         REGEX_EXTRACT(line, '^(\\\\S+)\\\\s+\\\\S+\\\\s+\\\\S+\\\\s+\\\\[(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2}):', 2), '-',\n" +
                "         REGEX_EXTRACT(line, '^(\\\\S+)\\\\s+\\\\S+\\\\s+\\\\S+\\\\s+\\\\[(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2}):', 4)) AS log_date,\n" +
                "  (int)REGEX_EXTRACT(line, '^(\\\\S+)\\\\s+\\\\S+\\\\s+\\\\S+\\\\s+\\\\[(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2}):', 5) AS log_hour,\n" +
                "  (int)REGEX_EXTRACT(line, '\\\\s+(\\\\d{3})\\\\s+', 0) AS status_code,\n" +
                "  REGEX_EXTRACT(line, '^(\\\\S+)\\\\s+\\\\S+\\\\s+\\\\S+\\\\s+\\\\[(\\\\d{2})/(\\\\w{3})/(\\\\d{4}):(\\\\d{2}):\\\\d{2}:\\\\d{2}', 1) AS host;\n" +
                "\n" +
                "-- Filter out malformed records\n" +
                "filtered = FILTER parsed BY log_date IS NOT NULL AND status_code IS NOT NULL;\n" +
                "\n" +
                "-- Group by log_date and log_hour\n" +
                "grouped = GROUP filtered BY (log_date, log_hour);\n" +
                "\n" +
                "-- Aggregate\n" +
                "result = FOREACH grouped GENERATE\n" +
                "  group.log_date AS log_date,\n" +
                "  group.log_hour AS log_hour,\n" +
                "  SUM(CASE WHEN filtered.status_code >= 400 AND filtered.status_code <= 599 THEN 1 ELSE 0 END) AS error_request_count,\n" +
                "  COUNT(filtered) AS total_request_count,\n" +
                "  (double)SUM(CASE WHEN filtered.status_code >= 400 AND filtered.status_code <= 599 THEN 1 ELSE 0 END) / COUNT(filtered) AS error_rate,\n" +
                "  COUNT(DISTINCT CASE WHEN filtered.status_code >= 400 AND filtered.status_code <= 599 THEN filtered.host ELSE NULL END) AS distinct_error_hosts;\n" +
                "\n" +
                "STORE result INTO '%s' USING PigStorage(',');",
                batchFilePath, outputPath);
    }

    public static void writePigScript(String scriptContent, Path scriptPath) throws Exception {
        Files.write(scriptPath, scriptContent.getBytes());
    }
}
