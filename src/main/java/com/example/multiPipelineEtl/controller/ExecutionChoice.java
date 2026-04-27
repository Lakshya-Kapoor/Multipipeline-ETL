package com.example.multiPipelineEtl.controller;

public enum ExecutionChoice {
    PIG,
    MAPREDUCE,
    MONGODB,
    HIVE;

    public static ExecutionChoice fromSelection(int selection) {
        switch (selection) {
            case 1:
                return PIG;
            case 2:
                return MAPREDUCE;
            case 3:
                return MONGODB;
            case 4:
                return HIVE;
            default:
                throw new IllegalArgumentException("Invalid execution choice: " + selection);
        }
    }
}
