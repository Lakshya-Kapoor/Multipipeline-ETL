package com.example.multipipelineetl.pipeline.pig;

import com.example.multipipelineetl.common.BatchFile;
import com.example.multipipelineetl.common.PigScriptGenerator;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.Query3Result;
import com.example.multipipelineetl.persistence.QueryResultRepository;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

public class PigQuery3Pipeline {
    private static final String OUTPUT_RELATIVE = "target/pig_results/query3_batch_";

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
        String pigScript = PigScriptGenerator.generateQuery3Script(batchFilePath, outputPath);

        // Create temp script file
        Path scriptPath = Paths.get("target", "pig_scripts", "query3_batch_" + batch.getBatchId() + ".pig");
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
            List<Query3Result> results = readQuery3Results(outputPath);

            // Load results to PostgreSQL
            resultRepository.insertQuery3(context.getRunId(), batch.getBatchId(), "PIG", results);

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

    private List<Query3Result> readQuery3Results(String outputPath) throws Exception {
        List<Query3Result> results = new ArrayList<>();
        Path outputFile = Paths.get(outputPath, "part-r-00000");

        if (Files.exists(outputFile)) {
            List<String> lines = Files.readAllLines(outputFile);
            for (String line : lines) {
                String[] parts = line.split(",");
                if (parts.length >= 6) {
                    String logDateStr = parts[0].trim();
                    int logHour = Integer.parseInt(parts[1].trim());
                    long errorCount = Long.parseLong(parts[2].trim());
                    long totalCount = Long.parseLong(parts[3].trim());
                    double errorRate = Double.parseDouble(parts[4].trim());
                    long distinctErrorHosts = Long.parseLong(parts[5].trim());

                    String normalizedDate = normalizeDate(logDateStr);
                    results.add(new Query3Result(
                            Date.valueOf(normalizedDate),
                            logHour,
                            errorCount,
                            totalCount,
                            errorRate,
                            distinctErrorHosts));
                }
            }
        }
        return results;
    }

    private String normalizeDate(String dateStr) {
        // Convert "Jul-01-1995" to "1995-07-01"
        // Map month names to numbers and rearrange
        dateStr = dateStr.replace("Jan", "01").replace("Feb", "02").replace("Mar", "03").replace("Apr", "04")
                         .replace("May", "05").replace("Jun", "06").replace("Jul", "07").replace("Aug", "08")
                         .replace("Sep", "09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12");
        // Now format is MM-DD-YYYY, need to convert to YYYY-MM-DD
        String[] parts = dateStr.split("-");
        if (parts.length == 3) {
            return parts[2] + "-" + parts[0] + "-" + parts[1];  // YYYY-MM-DD
        }
        return dateStr;
    }
}
