package com.example.multipipelineetl.pipeline.mapreduce.query3;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query3Result;
import com.example.multipipelineetl.persistence.QueryResultRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MapReduceQuery3Pipeline {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public void execute(BatchFile batch, ExecutionContext context, QueryResultRepository resultRepository,
                        Configuration hadoopConfig) throws Exception {
        String jobName = "query3_batch_" + batch.getBatchId();
        String outputPath = "target/mapreduce/" + jobName;
        
        try {
            // Delete output directory if exists
            FileSystem fs = FileSystem.get(hadoopConfig);
            Path outputDir = new Path(outputPath);
            if (fs.exists(outputDir)) {
                fs.delete(outputDir, true);
            }

            // Configure and run MapReduce job
            Job job = Job.getInstance(hadoopConfig, jobName);
            job.setJarByClass(MapReduceQuery3Pipeline.class);

            // Set mapper and reducer
            job.setMapperClass(Query3Mapper.class);
            job.setReducerClass(Query3Reducer.class);

            // Set key-value output types
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Query3Writable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Query3Writable.class);

            // Set input and output formats
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Set input path
            FileInputFormat.addInputPath(job, new Path(batch.getPath().toString()));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            // Run job
            boolean success = job.waitForCompletion(true);
            if (!success) {
                throw new IOException("MapReduce job for Query3 failed");
            }

            // Read results from output
            List<Query3Result> results = readResults(fs, outputDir);

            // Load results to PostgreSQL
            resultRepository.insertQuery3(context.getRunId(), batch.getBatchId(), "MAPREDUCE", results);

        } finally {
            // Cleanup
            cleanup(outputPath);
        }
    }

    private List<Query3Result> readResults(FileSystem fs, Path outputDir) throws Exception {
        List<Query3Result> results = new ArrayList<>();
        
        // Use SequenceFile reader to read MapReduce output
        org.apache.hadoop.io.SequenceFile.Reader reader = 
            new org.apache.hadoop.io.SequenceFile.Reader(fs.getConf(), 
                org.apache.hadoop.io.SequenceFile.Reader.file(new Path(outputDir, "part-r-00000")));
        
        Text key = new Text();
        Query3Writable value = new Query3Writable();
        
        try {
            while (reader.next(key, value)) {
                String[] parts = key.toString().split("\\|");
                LocalDate logDate = LocalDate.parse(parts[0], DATE_FORMATTER);
                int logHour = Integer.parseInt(parts[1]);
                
                results.add(new Query3Result(
                        Date.valueOf(logDate),
                        logHour,
                        value.getErrorCount(),
                        value.getTotalCount(),
                        value.getErrorRate(),
                        value.getDistinctErrorHostCount()
                ));
            }
        } finally {
            reader.close();
        }
        
        return results;
    }

    private void cleanup(String outputPath) {
        try {
            java.nio.file.Path path = Paths.get(outputPath);
            if (Files.exists(path)) {
                Files.walk(path)
                        .sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (Exception ignored) {
                            }
                        });
            }
        } catch (Exception ignored) {
        }
    }
}
