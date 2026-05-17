package com.example.multipipelineetl.pipeline.mapreduce.query3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Query3Reducer extends Reducer<Text, Query3Writable, Text, Query3Writable> {

    @Override
    public void reduce(Text key, Iterable<Query3Writable> values, Context context) throws IOException, InterruptedException {
        Query3Writable aggregate = new Query3Writable(0, 0, null);
        
        for (Query3Writable value : values) {
            aggregate.add(value);
        }
        
        context.write(key, aggregate);
    }
}
