package com.example.multipipelineetl.pipeline.mapreduce.query3;

import com.example.multipipelineetl.common.NasaLogParser;
import com.example.multipipelineetl.common.ParsedLogRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

public class Query3Mapper extends Mapper<LongWritable, Text, Text, Query3Writable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Optional<ParsedLogRecord> parsed = NasaLogParser.parse(value.toString());
        
        if (parsed.isPresent()) {
            ParsedLogRecord record = parsed.get();
            boolean isError = record.getStatusCode() >= 400 && record.getStatusCode() <= 599;
            
            String mapKey = record.getLogDate() + "|" + record.getLogHour();
            
            // Emit error count (1 if error, 0 otherwise) and total count (always 1)
            long errorCount = isError ? 1 : 0;
            String errorHost = isError ? record.getHost() : null;
            
            Query3Writable writable = new Query3Writable(errorCount, 1, errorHost);
            context.write(new Text(mapKey), writable);
        }
    }
}
