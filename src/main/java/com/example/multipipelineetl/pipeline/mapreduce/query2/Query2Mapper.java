package com.example.multipipelineetl.pipeline.mapreduce.query2;

import com.example.multipipelineetl.common.NasaLogParser;
import com.example.multipipelineetl.common.ParsedLogRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

public class Query2Mapper extends Mapper<LongWritable, Text, Text, Query2Writable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Optional<ParsedLogRecord> parsed = NasaLogParser.parse(value.toString());
        
        if (parsed.isPresent()) {
            ParsedLogRecord record = parsed.get();
            Query2Writable writable = new Query2Writable(1, record.getBytes(), record.getHost());
            context.write(new Text(record.getResourcePath()), writable);
        }
    }
}
