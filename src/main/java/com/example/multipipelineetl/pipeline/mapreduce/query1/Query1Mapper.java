package com.example.multipipelineetl.pipeline.mapreduce.query1;

import com.example.multipipelineetl.common.NasaLogParser;
import com.example.multipipelineetl.common.ParsedLogRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

public class Query1Mapper extends Mapper<LongWritable, Text, Text, Query1Writable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Optional<ParsedLogRecord> parsed = NasaLogParser.parse(value.toString());
        
        if (parsed.isPresent()) {
            ParsedLogRecord record = parsed.get();
            String mapKey = record.getLogDate() + "|" + record.getStatusCode();
            Query1Writable writable = new Query1Writable(1, record.getBytes());
            context.write(new Text(mapKey), writable);
        }
    }
}
