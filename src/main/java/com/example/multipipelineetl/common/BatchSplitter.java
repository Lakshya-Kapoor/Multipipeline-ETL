package com.example.multipipelineetl.common;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BatchSplitter {
    public List<BatchFile> split(Path datasetPath, int batchSize, Path outputDirectory) throws IOException {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
        Files.createDirectories(outputDirectory);
        List<String> lines = Files.readAllLines(datasetPath, StandardCharsets.UTF_8);
        List<BatchFile> batches = new ArrayList<BatchFile>();
        int batchId = 1;
        for (int start = 0; start < lines.size(); start += batchSize) {
            int end = Math.min(start + batchSize, lines.size());
            Path batchPath = outputDirectory.resolve("batch_" + batchId + ".log");
            BufferedWriter writer = Files.newBufferedWriter(batchPath, StandardCharsets.UTF_8);
            try {
                for (int i = start; i < end; i++) {
                    writer.write(lines.get(i));
                    writer.newLine();
                }
            } finally {
                writer.close();
            }
            batches.add(new BatchFile(batchId, batchPath, end - start));
            batchId++;
        }
        return batches;
    }
}

