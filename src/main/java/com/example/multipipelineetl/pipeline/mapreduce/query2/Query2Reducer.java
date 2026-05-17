package com.example.multipipelineetl.pipeline.mapreduce.query2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Query2Reducer extends Reducer<Text, Query2Writable, Text, Query2Writable> {

    @Override
    public void reduce(Text key, Iterable<Query2Writable> values, Context context) throws IOException, InterruptedException {
        Query2Writable aggregate = new Query2Writable(0, 0, null);
        
        for (Query2Writable value : values) {
            aggregate.add(value);
        }
        
        context.write(key, aggregate);
    }
}
