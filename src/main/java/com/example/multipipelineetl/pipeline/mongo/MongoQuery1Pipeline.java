package com.example.multipipelineetl.pipeline.mongo;

import com.example.multipipelineetl.common.ParsedLogRecord;
import com.example.multipipelineetl.model.Query1Result;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoQuery1Pipeline {
    public List<Query1Result> aggregate(List<ParsedLogRecord> parsedRows) {
        Map<String, long[]> aggregated = new HashMap<String, long[]>();
        for (ParsedLogRecord row : parsedRows) {
            String key = row.getLogDate().toString() + "|" + row.getStatusCode();
            long[] stats = aggregated.get(key);
            if (stats == null) {
                stats = new long[]{0L, 0L};
                aggregated.put(key, stats);
            }
            stats[0]++;
            stats[1] += row.getBytes();
        }
        List<Query1Result> results = new ArrayList<Query1Result>();
        for (Map.Entry<String, long[]> entry : aggregated.entrySet()) {
            String[] keyParts = entry.getKey().split("\\|");
            long[] stats = entry.getValue();
            results.add(new Query1Result(Date.valueOf(keyParts[0]), Integer.parseInt(keyParts[1]), stats[0], stats[1]));
        }
        return results;
    }
}

