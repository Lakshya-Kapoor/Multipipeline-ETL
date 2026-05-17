package com.example.multipipelineetl.pipeline.mapreduce.query1;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query1Result;
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

public class MapReduceQuery1Pipeline {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public void execute(BatchFile batch, ExecutionContext context, QueryResultRepository resultRepository,
                        Configuration hadoopConfig) throws Exception {
        String jobName = "query1_batch_" + batch.getBatchId();
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
            job.setJarByClass(MapReduceQuery1Pipeline.class);

            // Set mapper and reducer
            job.setMapperClass(Query1Mapper.class);
            job.setReducerClass(Query1Reducer.class);

            // Set key-value output types
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Query1Writable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Query1Writable.class);

            // Set input and output formats
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Set input path
            FileInputFormat.addInputPath(job, new Path(batch.getPath().toString()));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            // Run job
            boolean success = job.waitForCompletion(true);
            if (!success) {
                throw new IOException("MapReduce job for Query1 failed");
            }

            // Read results from output
            List<Query1Result> results = readResults(fs, outputDir);

            // Load results to PostgreSQL
            resultRepository.insertQuery1(context.getRunId(), batch.getBatchId(), "MAPREDUCE", results);

        } finally {
            // Cleanup
            cleanup(outputPath);
        }
    }

    private List<Query1Result> readResults(FileSystem fs, Path outputDir) throws Exception {
        List<Query1Result> results = new ArrayList<>();
        
        // Use SequenceFile reader to read MapReduce output
        org.apache.hadoop.io.SequenceFile.Reader reader = 
            new org.apache.hadoop.io.SequenceFile.Reader(fs.getConf(), 
                org.apache.hadoop.io.SequenceFile.Reader.file(new Path(outputDir, "part-r-00000")));
        
        Text key = new Text();
        Query1Writable value = new Query1Writable();
        
        try {
            while (reader.next(key, value)) {
                String[] parts = key.toString().split("\\|");
                LocalDate logDate = LocalDate.parse(parts[0], DATE_FORMATTER);
                int statusCode = Integer.parseInt(parts[1]);
                
                results.add(new Query1Result(
                        Date.valueOf(logDate),
                        statusCode,
                        value.getCount(),
                        value.getTotalBytes()
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
