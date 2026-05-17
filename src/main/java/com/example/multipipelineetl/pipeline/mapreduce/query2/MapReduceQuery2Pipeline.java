package com.example.multipipelineetl.pipeline.mapreduce.query2;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query2Result;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MapReduceQuery2Pipeline {

    public void execute(BatchFile batch, ExecutionContext context, QueryResultRepository resultRepository,
                        Configuration hadoopConfig) throws Exception {
        String jobName = "query2_batch_" + batch.getBatchId();
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
            job.setJarByClass(MapReduceQuery2Pipeline.class);

            // Set mapper and reducer
            job.setMapperClass(Query2Mapper.class);
            job.setReducerClass(Query2Reducer.class);

            // Set key-value output types
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Query2Writable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Query2Writable.class);

            // Set input and output formats
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Set input path
            FileInputFormat.addInputPath(job, new Path(batch.getPath().toString()));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            // Run job
            boolean success = job.waitForCompletion(true);
            if (!success) {
                throw new IOException("MapReduce job for Query2 failed");
            }

            // Read results from output
            List<Query2Result> results = readResults(fs, outputDir);

            // Sort by request_count descending and take top 20
            Collections.sort(results, (a, b) -> Long.compare(b.getRequestCount(), a.getRequestCount()));
            if (results.size() > 20) {
                results = results.subList(0, 20);
            }

            // Load results to PostgreSQL
            resultRepository.insertQuery2(context.getRunId(), batch.getBatchId(), "MAPREDUCE", results);

        } finally {
            // Cleanup
            cleanup(outputPath);
        }
    }

    private List<Query2Result> readResults(FileSystem fs, Path outputDir) throws Exception {
        List<Query2Result> results = new ArrayList<>();
        
        // Use SequenceFile reader to read MapReduce output
        org.apache.hadoop.io.SequenceFile.Reader reader = 
            new org.apache.hadoop.io.SequenceFile.Reader(fs.getConf(), 
                org.apache.hadoop.io.SequenceFile.Reader.file(new Path(outputDir, "part-r-00000")));
        
        Text key = new Text();
        Query2Writable value = new Query2Writable();
        
        try {
            while (reader.next(key, value)) {
                results.add(new Query2Result(
                        key.toString(),
                        value.getCount(),
                        value.getTotalBytes(),
                        value.getDistinctHostCount()
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
