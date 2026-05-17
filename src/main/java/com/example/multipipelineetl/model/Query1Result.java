package com.example.multipipelineetl.model;

import java.sql.Date;

public class Query1Result {
    private final Date logDate;
    private final int statusCode;
    private final long requestCount;
    private final long totalBytes;

    public Query1Result(Date logDate, int statusCode, long requestCount, long totalBytes) {
        this.logDate = logDate;
        this.statusCode = statusCode;
        this.requestCount = requestCount;
        this.totalBytes = totalBytes;
    }

    public Date getLogDate() {
        return logDate;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public long getRequestCount() {
        return requestCount;
    }

    public long getTotalBytes() {
        return totalBytes;
    }
}

