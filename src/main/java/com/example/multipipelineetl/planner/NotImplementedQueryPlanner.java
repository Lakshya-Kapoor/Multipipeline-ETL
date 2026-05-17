package com.example.multipipelineetl.planner;

import com.example.multipipelineetl.model.ExecutionContext;

public class NotImplementedQueryPlanner implements QueryPlanner {
    private final String plannerName;

    public NotImplementedQueryPlanner(String plannerName) {
        this.plannerName = plannerName;
    }

    public void setup(ExecutionContext context) {
    }

    public void execute(ExecutionContext context) {
        throw new UnsupportedOperationException(plannerName + " is planned for Phase 2.");
    }

    public void cleanup(ExecutionContext context) {
    }
}

