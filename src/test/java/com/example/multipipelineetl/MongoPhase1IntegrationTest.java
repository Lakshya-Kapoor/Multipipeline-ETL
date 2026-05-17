package com.example.multipipelineetl;

import com.example.multipipelineetl.model.ExecutionRequest;
import com.example.multipipelineetl.model.PipelineType;
import com.example.multipipelineetl.model.QueryType;
import com.example.multipipelineetl.orchestrator.PipelineOrchestrator;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MongoPhase1IntegrationTest extends TestCase {
    public MongoPhase1IntegrationTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(MongoPhase1IntegrationTest.class);
    }

    public void testPhase1Scaffold() {
        // Verify project structure and enum values are accessible
        assertNotNull(PipelineType.MONGO);
        assertNotNull(QueryType.QUERY1);
        assertNotNull(QueryType.ALL);
        assertTrue(true);
    }

    public void testExecutionRequestCreation() {
        ExecutionRequest request = new ExecutionRequest(
                PipelineType.MONGO,
                QueryType.QUERY1,
                1000,
                "sample_logs.txt");
        assertEquals(PipelineType.MONGO, request.getPipelineType());
        assertEquals(QueryType.QUERY1, request.getQueryType());
        assertEquals(1000, request.getBatchSize());
        assertEquals("sample_logs.txt", request.getDatasetPath());
    }
}
