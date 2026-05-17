package com.example.multipipelineetl.pipeline.mongo;

import com.example.multipipelineetl.common.ParsedLogRecord;
import com.example.multipipelineetl.model.Query3Result;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoQuery3Pipeline {
    public List<Query3Result> aggregate(List<ParsedLogRecord> parsedRows) {
        Map<String, long[]> counters = new HashMap<String, long[]>();
        Map<String, Set<String>> errorHosts = new HashMap<String, Set<String>>();

        for (ParsedLogRecord row : parsedRows) {
            String key = row.getLogDate().toString() + "|" + row.getLogHour();
            long[] stats = counters.get(key);
            if (stats == null) {
                stats = new long[]{0L, 0L};
                counters.put(key, stats);
            }
            stats[1]++;
            if (row.getStatusCode() >= 400 && row.getStatusCode() <= 599) {
                stats[0]++;
                Set<String> hosts = errorHosts.get(key);
                if (hosts == null) {
                    hosts = new HashSet<String>();
                    errorHosts.put(key, hosts);
                }
                hosts.add(row.getHost());
            }
        }

        List<Query3Result> results = new ArrayList<Query3Result>();
        for (Map.Entry<String, long[]> entry : counters.entrySet()) {
            String[] keyParts = entry.getKey().split("\\|");
            long[] stats = entry.getValue();
            long errorCount = stats[0];
            long totalCount = stats[1];
            double rate = totalCount == 0 ? 0.0 : ((double) errorCount / (double) totalCount);
            Set<String> hosts = errorHosts.get(entry.getKey());
            long distinctErrorHosts = hosts == null ? 0L : hosts.size();
            results.add(new Query3Result(Date.valueOf(keyParts[0]), Integer.parseInt(keyParts[1]), errorCount, totalCount, rate, distinctErrorHosts));
        }
        return results;
    }
}

