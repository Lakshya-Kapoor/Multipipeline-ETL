package com.example.multipipelineetl.model;

import java.sql.Date;

public class Query3Result {
    private final Date logDate;
    private final int logHour;
    private final long errorRequestCount;
    private final long totalRequestCount;
    private final double errorRate;
    private final long distinctErrorHosts;

    public Query3Result(Date logDate, int logHour, long errorRequestCount, long totalRequestCount, double errorRate, long distinctErrorHosts) {
        this.logDate = logDate;
        this.logHour = logHour;
        this.errorRequestCount = errorRequestCount;
        this.totalRequestCount = totalRequestCount;
        this.errorRate = errorRate;
        this.distinctErrorHosts = distinctErrorHosts;
    }

    public Date getLogDate() {
        return logDate;
    }

    public int getLogHour() {
        return logHour;
    }

    public long getErrorRequestCount() {
        return errorRequestCount;
    }

    public long getTotalRequestCount() {
        return totalRequestCount;
    }

    public double getErrorRate() {
        return errorRate;
    }

    public long getDistinctErrorHosts() {
        return distinctErrorHosts;
    }
}

