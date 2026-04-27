package com.example.multiPipelineEtl.pipeline.query1;

public class ParsedLogRecord {
    private final String logDate;
    private final int statusCode;
    private final long bytesTransferred;

    public ParsedLogRecord(String logDate, int statusCode, long bytesTransferred) {
        if (logDate == null || logDate.trim().isEmpty()) {
            throw new IllegalArgumentException("logDate cannot be null or empty");
        }
        this.logDate = logDate;
        this.statusCode = statusCode;
        this.bytesTransferred = bytesTransferred;
    }

    public String getLogDate() {
        return logDate;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public long getBytesTransferred() {
        return bytesTransferred;
    }
}
