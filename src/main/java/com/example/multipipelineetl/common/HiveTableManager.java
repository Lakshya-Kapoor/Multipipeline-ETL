package com.example.multipipelineetl.common;

import java.sql.Connection;
import java.sql.Statement;

public class HiveTableManager {
    private static final String REGEX_PATTERN =
            "^([\\S]+) ([\\S]+) ([\\S]+) \\[([^\\]]+)\\] \"([A-Z]+) ([\\S]+) ([\\S]+)\" (-|\\d+) (-|\\d+)";

    public static void createExternalTable(Connection hiveConn, String tableName, String filePath) throws Exception {
        String createTableSQL = String.format(
                "CREATE EXTERNAL TABLE IF NOT EXISTS %s (\n" +
                "  host STRING,\n" +
                "  identity STRING,\n" +
                "  user STRING,\n" +
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

    public static void createTransformationView(Connection hiveConn, String viewName, String sourceTable) throws Exception {
        String createViewSQL = String.format(
                "CREATE TEMPORARY VIEW %s AS\n" +
                "SELECT\n" +
                "  host,\n" +
                "  FROM_UNIXTIME(UNIX_TIMESTAMP(log_timestamp, 'dd/MMM/yyyy:HH:mm:ss Z'), 'yyyy-MM-dd') AS log_date,\n" +
                "  HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(log_timestamp, 'dd/MMM/yyyy:HH:mm:ss Z'))) AS log_hour,\n" +
                "  status_code,\n" +
                "  CASE WHEN bytes_transferred = -1 THEN 0 ELSE bytes_transferred END AS bytes,\n" +
                "  resource_path\n" +
                "FROM %s",
                viewName, sourceTable);

        try (Statement stmt = hiveConn.createStatement()) {
            stmt.execute(createViewSQL);
        }
    }
}
