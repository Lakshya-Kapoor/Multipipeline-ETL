package com.example.multipipelineetl.model;

public class Query2Result {
    private final String resourcePath;
    private final long requestCount;
    private final long totalBytes;
    private final long distinctHostCount;

    public Query2Result(String resourcePath, long requestCount, long totalBytes, long distinctHostCount) {
        this.resourcePath = resourcePath;
        this.requestCount = requestCount;
        this.totalBytes = totalBytes;
        this.distinctHostCount = distinctHostCount;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public long getRequestCount() {
        return requestCount;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public long getDistinctHostCount() {
        return distinctHostCount;
    }
}

