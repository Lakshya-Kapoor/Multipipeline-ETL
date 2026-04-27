package com.example.multiPipelineEtl.controller;

import java.io.PrintStream;
import java.util.Scanner;

public class ExecutionController {
    private final Scanner scanner;
    private final PrintStream out;

    public ExecutionController(Scanner scanner, PrintStream out) {
        if (scanner == null) {
            throw new IllegalArgumentException("scanner cannot be null");
        }
        if (out == null) {
            throw new IllegalArgumentException("out cannot be null");
        }
        this.scanner = scanner;
        this.out = out;
    }

    public ExecutionRequest collectExecutionRequest() {
        ExecutionChoice executionChoice = selectExecutionChoice();
        int numberOfBatches = selectNumberOfBatches();
        return new ExecutionRequest(executionChoice, numberOfBatches);
    }

    private ExecutionChoice selectExecutionChoice() {
        while (true) {
            out.println("Select execution choice:");
            out.println("1. Pig");
            out.println("2. MapReduce");
            out.println("3. MongoDB");
            out.println("4. Hive");
            out.print("> ");

            String rawInput = scanner.nextLine();
            int selection;
            try {
                selection = Integer.parseInt(rawInput.trim());
            } catch (NumberFormatException ex) {
                out.println("Invalid selection. Enter a number between 1 and 4.");
                continue;
            }

            try {
                return ExecutionChoice.fromSelection(selection);
            } catch (IllegalArgumentException ex) {
                out.println(ex.getMessage());
            }
        }
    }

    private int selectNumberOfBatches() {
        while (true) {
            out.print("Enter number of batches (> 0): ");
            String rawInput = scanner.nextLine();
            int numberOfBatches;
            try {
                numberOfBatches = Integer.parseInt(rawInput.trim());
            } catch (NumberFormatException ex) {
                out.println("Invalid batch count. Enter a positive integer.");
                continue;
            }

            if (numberOfBatches <= 0) {
                out.println("Batch count must be greater than zero.");
                continue;
            }
            return numberOfBatches;
        }
    }
}
