package com.example.multipipelineetl.pipeline.mapreduce.query2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Query2Writable implements Writable {
    private long count;
    private long totalBytes;
    private Set<String> distinctHosts;

    public Query2Writable() {
        this.distinctHosts = new HashSet<>();
    }

    public Query2Writable(long count, long totalBytes, String host) {
        this.count = count;
        this.totalBytes = totalBytes;
        this.distinctHosts = new HashSet<>();
        if (host != null && !host.isEmpty()) {
            this.distinctHosts.add(host);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeLong(totalBytes);
        out.writeInt(distinctHosts.size());
        for (String host : distinctHosts) {
            out.writeUTF(host);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong();
        totalBytes = in.readLong();
        distinctHosts = new HashSet<>();
        int hostCount = in.readInt();
        for (int i = 0; i < hostCount; i++) {
            distinctHosts.add(in.readUTF());
        }
    }

    public long getCount() {
        return count;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public Set<String> getDistinctHosts() {
        return distinctHosts;
    }

    public long getDistinctHostCount() {
        return distinctHosts.size();
    }

    public void add(Query2Writable other) {
        this.count += other.count;
        this.totalBytes += other.totalBytes;
        this.distinctHosts.addAll(other.distinctHosts);
    }
}
