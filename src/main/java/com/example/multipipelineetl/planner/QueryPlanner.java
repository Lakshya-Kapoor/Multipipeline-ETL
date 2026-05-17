package com.example.multipipelineetl.planner;

import com.example.multipipelineetl.model.ExecutionContext;

public interface QueryPlanner {
    void setup(ExecutionContext context) throws Exception;
    void execute(ExecutionContext context) throws Exception;
    void cleanup(ExecutionContext context) throws Exception;
}

