package com.example.multipipelineetl.common;

import java.time.LocalDate;

public class ParsedLogRecord {
    private final String host;
    private final LocalDate logDate;
    private final int logHour;
    private final String resourcePath;
    private final int statusCode;
    private final long bytes;

    public ParsedLogRecord(String host, LocalDate logDate, int logHour, String resourcePath, int statusCode, long bytes) {
        this.host = host;
        this.logDate = logDate;
        this.logHour = logHour;
        this.resourcePath = resourcePath;
        this.statusCode = statusCode;
        this.bytes = bytes;
    }

    public String getHost() {
        return host;
    }

    public LocalDate getLogDate() {
        return logDate;
    }

    public int getLogHour() {
        return logHour;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public long getBytes() {
        return bytes;
    }
}

