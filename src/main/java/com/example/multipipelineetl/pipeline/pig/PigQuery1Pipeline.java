package com.example.multipipelineetl.pipeline.pig;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.PigScriptGenerator;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query1Result;
import com.example.multipipelineetl.persistence.QueryResultRepository;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

public class PigQuery1Pipeline {
    private static final String OUTPUT_RELATIVE = "target/pig_results/query1_batch_";

    public void execute(BatchFile batch, ExecutionContext context, QueryResultRepository resultRepository,
            Connection postgresConnection) throws Exception {
        String outputPath = OUTPUT_RELATIVE + batch.getBatchId();
        Path outputDir = Paths.get(outputPath);

        // Clean up output directory if it exists
        if (Files.exists(outputDir)) {
            Files.walk(outputDir)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception ignored) {
                        }
                    });
        }

        String batchFilePath = batch.getPath().toString();

        // Generate Pig Latin script
        String pigScript = PigScriptGenerator.generateQuery1Script(batchFilePath, outputPath);

        // Create temp script file
        Path scriptPath = Paths.get("target", "pig_scripts", "query1_batch_" + batch.getBatchId() + ".pig");
        Files.createDirectories(scriptPath.getParent());
        PigScriptGenerator.writePigScript(pigScript, scriptPath);

        try {
            // Execute Pig script via CLI in local mode
            ProcessBuilder pb = new ProcessBuilder(
                "pig",
                "-x", "local",
                scriptPath.toString()
            );
            
            pb.inheritIO();  // Show Pig output
            int exitCode = pb.start().waitFor();
            
            if (exitCode != 0) {
                throw new Exception("Pig script execution failed with exit code: " + exitCode);
            }

            // Read results from output
            List<Query1Result> results = readQuery1Results(outputPath);

            // Load results to PostgreSQL
            resultRepository.insertQuery1(context.getRunId(), batch.getBatchId(), "PIG", results);

        } finally {
            // Cleanup output directory
            if (Files.exists(outputDir)) {
                Files.walk(outputDir)
                        .sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (Exception ignored) {
                            }
                        });
            }
            // Cleanup script file
            if (Files.exists(scriptPath)) {
                Files.deleteIfExists(scriptPath);
            }
        }
    }

    private List<Query1Result> readQuery1Results(String outputPath) throws Exception {
        List<Query1Result> results = new ArrayList<>();
        Path outputFile = Paths.get(outputPath, "part-r-00000");

        if (Files.exists(outputFile)) {
            List<String> lines = Files.readAllLines(outputFile);
            for (String line : lines) {
                String[] parts = line.split(",");
                if (parts.length >= 4) {
                    String logDateStr = parts[0].trim();
                    int statusCode = Integer.parseInt(parts[1].trim());
                    long requestCount = Long.parseLong(parts[2].trim());
                    long totalBytes = Long.parseLong(parts[3].trim());

                    results.add(new Query1Result(
                            Date.valueOf(logDateStr),
                            statusCode,
                            requestCount,
                            totalBytes));
                }
            }
        }
        return results;
    }
}
