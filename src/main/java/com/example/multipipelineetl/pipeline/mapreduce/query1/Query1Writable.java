package com.example.multipipelineetl.pipeline.mapreduce.query1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Query1Writable implements Writable {
    private long count;
    private long totalBytes;

    public Query1Writable() {
    }

    public Query1Writable(long count, long totalBytes) {
        this.count = count;
        this.totalBytes = totalBytes;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeLong(totalBytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong();
        totalBytes = in.readLong();
    }

    public long getCount() {
        return count;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public void add(Query1Writable other) {
        this.count += other.count;
        this.totalBytes += other.totalBytes;
    }
}
