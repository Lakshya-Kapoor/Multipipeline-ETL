package com.example.multiPipelineEtl.pipeline.contracts;

import java.io.IOException;
import java.sql.SQLException;

import com.example.multiPipelineEtl.pipeline.common.QueryExecutionContext;

public interface Query1Pipeline {
    void run(int numberOfBatches, QueryExecutionContext context) throws IOException, SQLException;
}
