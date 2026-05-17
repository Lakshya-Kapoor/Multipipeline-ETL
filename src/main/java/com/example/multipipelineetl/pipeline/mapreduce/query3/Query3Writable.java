package com.example.multipipelineetl.pipeline.mapreduce.query3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Query3Writable implements Writable {
    private long errorCount;
    private long totalCount;
    private Set<String> distinctErrorHosts;

    public Query3Writable() {
        this.distinctErrorHosts = new HashSet<>();
    }

    public Query3Writable(long errorCount, long totalCount, String errorHost) {
        this.errorCount = errorCount;
        this.totalCount = totalCount;
        this.distinctErrorHosts = new HashSet<>();
        if (errorHost != null && !errorHost.isEmpty()) {
            this.distinctErrorHosts.add(errorHost);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(errorCount);
        out.writeLong(totalCount);
        out.writeInt(distinctErrorHosts.size());
        for (String host : distinctErrorHosts) {
            out.writeUTF(host);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        errorCount = in.readLong();
        totalCount = in.readLong();
        distinctErrorHosts = new HashSet<>();
        int hostCount = in.readInt();
        for (int i = 0; i < hostCount; i++) {
            distinctErrorHosts.add(in.readUTF());
        }
    }

    public long getErrorCount() {
        return errorCount;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public long getDistinctErrorHostCount() {
        return distinctErrorHosts.size();
    }

    public double getErrorRate() {
        if (totalCount == 0) {
            return 0.0;
        }
        return (double) errorCount / totalCount;
    }

    public void add(Query3Writable other) {
        this.errorCount += other.errorCount;
        this.totalCount += other.totalCount;
        this.distinctErrorHosts.addAll(other.distinctErrorHosts);
    }
}
