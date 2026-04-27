package com.example.multiPipelineEtl.controller;

public class ExecutionRequest {
    private final ExecutionChoice executionChoice;
    private final int numberOfBatches;

    public ExecutionRequest(ExecutionChoice executionChoice, int numberOfBatches) {
        if (executionChoice == null) {
            throw new IllegalArgumentException("executionChoice cannot be null");
        }
        if (numberOfBatches <= 0) {
            throw new IllegalArgumentException("numberOfBatches must be greater than zero");
        }
        this.executionChoice = executionChoice;
        this.numberOfBatches = numberOfBatches;
    }

    public ExecutionChoice getExecutionChoice() {
        return executionChoice;
    }

    public int getNumberOfBatches() {
        return numberOfBatches;
    }
}
