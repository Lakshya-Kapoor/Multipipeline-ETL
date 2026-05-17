package com.example.multipipelineetl.pipeline.mongo;

import com.example.multipipelineetl.common.ParsedLogRecord;
import com.example.multipipelineetl.model.Query2Result;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoQuery2Pipeline {
    public List<Query2Result> aggregate(List<ParsedLogRecord> parsedRows) {
        Map<String, long[]> metrics = new HashMap<String, long[]>();
        Map<String, Set<String>> hosts = new HashMap<String, Set<String>>();
        for (ParsedLogRecord row : parsedRows) {
            String path = row.getResourcePath();
            long[] stats = metrics.get(path);
            if (stats == null) {
                stats = new long[]{0L, 0L};
                metrics.put(path, stats);
            }
            stats[0]++;
            stats[1] += row.getBytes();
            Set<String> hostSet = hosts.get(path);
            if (hostSet == null) {
                hostSet = new HashSet<String>();
                hosts.put(path, hostSet);
            }
            hostSet.add(row.getHost());
        }
        List<Query2Result> all = new ArrayList<Query2Result>();
        for (Map.Entry<String, long[]> entry : metrics.entrySet()) {
            String resource = entry.getKey();
            long[] stats = entry.getValue();
            all.add(new Query2Result(resource, stats[0], stats[1], hosts.get(resource).size()));
        }
        all.sort(new Comparator<Query2Result>() {
            public int compare(Query2Result left, Query2Result right) {
                return Long.compare(right.getRequestCount(), left.getRequestCount());
            }
        });
        return all.subList(0, Math.min(20, all.size()));
    }
}

