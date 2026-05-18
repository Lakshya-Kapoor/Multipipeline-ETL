package com.example.multipipelineetl.common;

import java.sql.Connection;
import java.sql.Statement;

public class HiveTableManager {
    private static final String REGEX_PATTERN =
            "^([\\S]+) [\\S]+ [\\S]+ \\[([^\\]]+)\\] \"([A-Z]+) ([\\S]+) ([\\S]+)\" (\\d+|-) (\\d+|-).*$";

    public static void createExternalTable(Connection hiveConn, String tableName, String filePath) throws Exception {
        String createTableSQL = String.format(
                "CREATE EXTERNAL TABLE IF NOT EXISTS %s (\n" +
                "  host STRING,\n" +
                "  log_timestamp STRING,\n" +
                "  http_method STRING,\n" +
                "  resource_path STRING,\n" +
                "  protocol STRING,\n" +
                "  status_code INT,\n" +
                "  bytes_transferred BIGINT\n" +
                ")\n" +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'\n" +
                "WITH SERDEPROPERTIES (\n" +
                "  'input.regex' = '%s'\n" +
                ")\n" +
                "STORED AS TEXTFILE\n" +
                "LOCATION '%s'",
                tableName, REGEX_PATTERN, filePath);

        try (Statement stmt = hiveConn.createStatement()) {
            stmt.execute(createTableSQL);
        }
    }

    public static void dropTable(Connection hiveConn, String tableName) throws Exception {
        String dropSQL = String.format("DROP TABLE IF EXISTS %s", tableName);
        try (Statement stmt = hiveConn.createStatement()) {
            stmt.execute(dropSQL);
        }
    }
}

