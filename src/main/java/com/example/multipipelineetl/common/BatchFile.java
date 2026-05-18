package com.example.multipipelineetl.common;

import java.nio.file.Path;

public class BatchFile {
    private final int batchId;
    private final Path path;
    private final String hdfsPath;
    private final long records;

    public BatchFile(int batchId, Path path, long records) {
        this.batchId = batchId;
        this.path = path;
        this.hdfsPath = null;
        this.records = records;
    }

    public BatchFile(int batchId, org.apache.hadoop.fs.Path hdfsPath, long records) {
        this.batchId = batchId;
        this.path = null;
        this.hdfsPath = hdfsPath.toString();
        this.records = records;
    }

    public int getBatchId() {
        return batchId;
    }

    public Path getPath() {
        return path;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public long getRecords() {
        return records;
    }
}

