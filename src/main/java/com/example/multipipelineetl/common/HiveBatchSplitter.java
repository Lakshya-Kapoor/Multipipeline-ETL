package com.example.multipipelineetl.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class HiveBatchSplitter {
    public List<BatchFile> split(java.nio.file.Path datasetPath, int batchSize, java.nio.file.Path outputDirectory) throws IOException {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
        
        // Create local output directory
        Files.createDirectories(outputDirectory);
        
        // Initialize HDFS
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "hdfs://sahas:9000");
        FileSystem hdfs = FileSystem.get(hadoopConf);
        
        List<String> lines = Files.readAllLines(datasetPath, StandardCharsets.ISO_8859_1);
        List<BatchFile> batches = new ArrayList<>();
        int batchId = 1;
        
        for (int start = 0; start < lines.size(); start += batchSize) {
            int end = Math.min(start + batchSize, lines.size());
            
            // Create local batch directory for reference
            java.nio.file.Path localBatchDir = outputDirectory.resolve("batch_" + batchId);
            Files.createDirectories(localBatchDir);
            
            // Write to HDFS instead of local filesystem
            Path hdfsBatchDir = new Path("/user/sahas/hive_batches/batch_" + batchId);
            hdfs.mkdirs(hdfsBatchDir);
            
            Path hdfsDataPath = new Path(hdfsBatchDir, "data.log");
            
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(hdfs.create(hdfsDataPath), StandardCharsets.ISO_8859_1))) {
                for (int i = start; i < end; i++) {
                    writer.write(lines.get(i));
                    writer.newLine();
                }
            }
            
            // Store HDFS path for Hive LOCATION clause
            batches.add(new BatchFile(batchId, hdfsBatchDir, end - start));
            batchId++;
        }
        
        return batches;
    }
}
