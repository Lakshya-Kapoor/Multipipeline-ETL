package com.example.multipipelineetl.pipeline.mapreduce.query1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Query1Reducer extends Reducer<Text, Query1Writable, Text, Query1Writable> {

    @Override
    public void reduce(Text key, Iterable<Query1Writable> values, Context context) throws IOException, InterruptedException {
        Query1Writable aggregate = new Query1Writable(0, 0);
        
        for (Query1Writable value : values) {
            aggregate.add(value);
        }
        
        context.write(key, aggregate);
    }
}
