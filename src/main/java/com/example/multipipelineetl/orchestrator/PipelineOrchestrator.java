package com.example.multipipelineetl.orchestrator;

import com.example.multipipelineetl.common.RunIdGenerator;
import com.example.multipipelineetl.model.ExecutionContext;
import com.example.multipipelineetl.model.ExecutionRequest;
import com.example.multipipelineetl.model.PipelineType;
import com.example.multipipelineetl.planner.NotImplementedQueryPlanner;
import com.example.multipipelineetl.planner.QueryPlanner;
import com.example.multipipelineetl.planner.mongo.MongoQueryPlanner;

import java.util.EnumMap;
import java.util.Map;

public class PipelineOrchestrator {
    private final Map<PipelineType, QueryPlanner> planners = new EnumMap<PipelineType, QueryPlanner>(PipelineType.class);

    public PipelineOrchestrator() {
        planners.put(PipelineType.MONGO, new MongoQueryPlanner());
        planners.put(PipelineType.MAPREDUCE, new NotImplementedQueryPlanner("MapReduceQueryPlanner"));
        planners.put(PipelineType.PIG, new NotImplementedQueryPlanner("PigQueryPlanner"));
        planners.put(PipelineType.HIVE, new NotImplementedQueryPlanner("HiveQueryPlanner"));
    }

    public long execute(ExecutionRequest request) throws Exception {
        long runId = RunIdGenerator.nextRunId();
        ExecutionContext context = new ExecutionContext(runId, request);
        QueryPlanner planner = planners.get(request.getPipelineType());
        if (planner == null) {
            throw new IllegalArgumentException("No planner registered for " + request.getPipelineType());
        }
        planner.setup(context);
        try {
            planner.execute(context);
        } finally {
            planner.cleanup(context);
        }
        return runId;
    }
}

