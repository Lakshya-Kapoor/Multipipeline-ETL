src/
├── main/
│   ├── java/com/example/multiPipelineEtl/
│   │   ├── App.java                             # Main application entry point
│   │   │
│   │   ├── controller/
│   │   │   ├── PipelineOrchestrator.java        # Orchestrates multi-pipeline execution flow
│   │   │   ├── ExecutionController.java         # Handles pipeline selection and execution
│   │   │   ├── ExecutionRequest.java            # Request DTO for pipeline execution parameters
│   │   │   └── ExecutionChoice.java             # Enum for available pipeline choices
│   │   │
│   │   ├── pipeline/
│   │   │   ├── contracts/
│   │   │   │   ├── Query1Pipeline.java          # Interface: Extract hosts with >10 requests
│   │   │   │   ├── Query2Pipeline.java          # Interface: Count different daily stats
│   │   │   │   └── Query3Pipeline.java          # Interface: Find failed HTTP responses by hour
│   │   │   │
│   │   │   ├── query1/
│   │   │   │   ├── AbstractQuery1Pipeline.java  # Base implementation with common Query1 logic
│   │   │   │   ├── MongoDbQuery1Pipeline.java   # MongoDB-specific Query1 implementation
│   │   │   │   └── ParsedLogRecord.java         # Data model for parsed HTTP log records
│   │   │   │
│   │   │   └── common/
│   │   │       ├── PipelineRuntimeMetrics.java  # Collects execution time and row count metrics
│   │   │       └── QueryExecutionContext.java   # Shared context passed through pipeline stages
│   │   │
│   │   ├── db/
│   │   │   ├── MongoDriverConfig.java           # MongoDB driver 4.11.1 configuration
│   │   │   ├── MongoDriverConnectionFactory.java # Creates MongoClient connections
│   │   │   ├── MongoJdbcConfig.java             # Legacy MongoDB JDBC wrapper config
│   │   │   ├── MongoJdbcConnectionFactory.java  # Legacy MongoDB JDBC connection factory
│   │   │   ├── PostgresConfig.java              # PostgreSQL connection configuration
│   │   │   └── PostgresConnectionFactory.java   # Creates PostgreSQL JDBC connections
│   │   │
│   │   └── reporting/
│   │       └── ReportingModule.java             # Formats and outputs pipeline results
│   │
│   └── resources/
│       └── sql/
│           └── schema.sql                       # SQL DDL for database initialization
│
└── test/
    └── java/com/example/multiPipelineEtl/
        ├── AppTest.java                         # Unit tests for main App class
        └── pipeline/query1/
            └── MongoDbQuery1PipelineTest.java   # Unit tests for MongoDB Query1 implementation
