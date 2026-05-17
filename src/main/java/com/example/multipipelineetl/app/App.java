package com.example.multipipelineetl.app;

import com.example.multipipelineetl.model.ExecutionRequest;
import com.example.multipipelineetl.model.PipelineType;
import com.example.multipipelineetl.model.QueryType;
import com.example.multipipelineetl.orchestrator.PipelineOrchestrator;
import com.example.multipipelineetl.reporting.ReportService;

import java.util.Scanner;

public class App {
    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        PipelineOrchestrator orchestrator = new PipelineOrchestrator();
        ReportService reportService = new ReportService();

        while (true) {
            System.out.println("1. Execute");
            System.out.println("2. Show Reports");
            System.out.println("3. Exit");
            String choice = scanner.nextLine().trim();
            if ("1".equals(choice)) {
                ExecutionRequest request = buildRequest(scanner);
                long runId = orchestrator.execute(request);
                System.out.println("Execution complete. run_id=" + runId);
            } else if ("2".equals(choice)) {
                System.out.print("Enter run_id: ");
                long runId = Long.parseLong(scanner.nextLine().trim());
                reportService.printRunSummary(runId);
            } else if ("3".equals(choice)) {
                break;
            } else {
                System.out.println("Invalid choice.");
            }
        }
    }

    private static ExecutionRequest buildRequest(Scanner scanner) {
        System.out.println("Select Pipeline: 1.Mongo 2.MapReduce 3.Pig 4.Hive");
        String pipelineChoice = scanner.nextLine().trim();
        PipelineType pipelineType;
        if ("1".equals(pipelineChoice)) {
            pipelineType = PipelineType.MONGO;
        } else if ("2".equals(pipelineChoice)) {
            pipelineType = PipelineType.MAPREDUCE;
        } else if ("3".equals(pipelineChoice)) {
            pipelineType = PipelineType.PIG;
        } else if ("4".equals(pipelineChoice)) {
            pipelineType = PipelineType.HIVE;
        } else {
            throw new IllegalArgumentException("Invalid pipeline selection.");
        }

        System.out.println("Select Query: 1.Query1 2.Query2 3.Query3 4.Execute All");
        String queryChoice = scanner.nextLine().trim();
        QueryType queryType;
        if ("1".equals(queryChoice)) {
            queryType = QueryType.QUERY1;
        } else if ("2".equals(queryChoice)) {
            queryType = QueryType.QUERY2;
        } else if ("3".equals(queryChoice)) {
            queryType = QueryType.QUERY3;
        } else if ("4".equals(queryChoice)) {
            queryType = QueryType.ALL;
        } else {
            throw new IllegalArgumentException("Invalid query selection.");
        }

        System.out.print("Batch size: ");
        int batchSize = Integer.parseInt(scanner.nextLine().trim());
        System.out.print("Dataset path: ");
        String datasetPath = scanner.nextLine().trim();
        return new ExecutionRequest(pipelineType, queryType, batchSize, datasetPath);
    }
}
