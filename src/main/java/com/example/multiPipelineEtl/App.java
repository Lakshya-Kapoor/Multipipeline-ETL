package com.example.multiPipelineEtl;

import java.util.Scanner;

import com.example.multiPipelineEtl.controller.ExecutionController;
import com.example.multiPipelineEtl.controller.ExecutionRequest;
import com.example.multiPipelineEtl.controller.PipelineOrchestrator;

public class App {
    public static void main(String[] args) {
        ExecutionController executionController = new ExecutionController(new Scanner(System.in), System.out);
        ExecutionRequest executionRequest = executionController.collectExecutionRequest();

        PipelineOrchestrator pipelineOrchestrator = new PipelineOrchestrator(System.out);
        pipelineOrchestrator.execute(executionRequest);
    }
}
