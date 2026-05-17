package com.example.multipipelineetl.common;

import java.nio.file.Path;

public class BatchFile {
    private final int batchId;
    private final Path path;
    private final long records;

    public BatchFile(int batchId, Path path, long records) {
        this.batchId = batchId;
        this.path = path;
        this.records = records;
    }

    public int getBatchId() {
        return batchId;
    }

    public Path getPath() {
        return path;
    }

    public long getRecords() {
        return records;
    }
}

