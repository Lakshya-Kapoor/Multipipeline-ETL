# Project Overview

The objective of this project is to build a unified ETL and analytics framework capable of executing the same web log analytics workload using four different execution technologies:

1. Hadoop MapReduce
2. Apache Pig
3. Apache Hive
4. MongoDB Aggregation Framework

The system must process the NASA HTTP Web Server Logs dataset and execute the same analytical queries across all execution technologies while preserving identical semantics, identical batching behavior, and identical output schemas.

The purpose of the project is not simply to compute analytical results, but to compare how different NoSQL and distributed processing technologies solve the same ETL problem. Therefore, every pipeline must implement equivalent extraction, transformation, aggregation, malformed-row handling, batching, and relational result loading logic.

The system is designed as a single Java application that orchestrates all execution technologies from one process. The Java application acts as the central controller responsible for:
- accepting execution requests,
- selecting execution technologies,
- managing batching,
- coordinating execution,
- collecting execution metadata,
- storing query results,
- generating reports.

The execution technologies themselves are responsible only for performing the actual ETL and aggregation work.

The project architecture separates orchestration logic from pipeline execution logic so that:
- execution technologies remain isolated,
- query semantics remain consistent,
- reporting remains technology-independent,
- the system can compare runtime and batching behavior fairly.

The architecture intentionally avoids preprocessing the dataset outside the selected execution technology because the project specification requires the selected pipeline itself to perform extraction, cleaning, transformation, and aggregation.
---

# Queries

## Query 1 — Daily Traffic Summary

For each:

* log_date
* status_code

Compute:

* request_count
* total_bytes

Output:

```text
log_date
status_code
request_count
total_bytes
```

---

## Query 2 — Top Requested Resources

Top 20 resource paths by request count.

For each resource:

* request_count
* total_bytes
* distinct_host_count

Output:

```text
resource_path
request_count
total_bytes
distinct_host_count
```

---

## Query 3 — Hourly Error Analysis

For each:

* log_date
* log_hour

Compute:

* error_request_count (status 400–599)
* total_request_count
* error_rate
* distinct_error_hosts

Output:

```text
log_date
log_hour
error_request_count
total_request_count
error_rate
distinct_error_hosts
```

Queries must remain logically identical across all pipelines. 

---

# High-Level Architecture

```text
+------------------------------------------------------+
|                      APP CLI                         |
+------------------------------------------------------+
                        |
                        v
+------------------------------------------------------+
|                Pipeline Orchestrator                 |
+------------------------------------------------------+
      |                 |                |
      v                 v                v
+-------------+  +-------------+  +-------------+
| QueryPlanner|  | QueryPlanner|  | QueryPlanner|
| MapReduce   |  | Pig         |  | Hive        |
+-------------+  +-------------+  +-------------+
      |
      v
+------------------------------------------------------+
|                  Query Pipelines                     |
|   (Q1/Q2/Q3 per execution technology)                |
+------------------------------------------------------+
      |
      v
+------------------------------------------------------+
|          Relational Result + Metadata DB             |
+------------------------------------------------------+
      |
      v
+------------------------------------------------------+
|                 Reporting Module                     |
+------------------------------------------------------+
```

---

# Architectural Principles

## 1. Single JVM Process

The entire system runs from one Java application.

No separate orchestrator scripts.

The Java process:

* accepts user commands,
* selects pipeline,
* configures batching,
* executes ETL,
* stores results,
* generates reports.
## 3. Pipeline Isolation

Each execution technology implements:

* its own extraction,
* transformation,
* aggregation logic.

The orchestrator only coordinates execution.

This is required by evaluation guidelines. 

# Proposed Modules

# 1. App Module

## Responsibility

Interactive CLI loop.

## Features

User selects:

* execution technology
* query
* batch size
* run mode

## Commands

Example:

```text
1. Select Pipeline
2. Select Query
3. Set Batch Size
4. Execute
5. Show Reports
6. Exit
```

---

# 2. Pipeline Orchestrator

The Pipeline Orchestrator is the central coordination layer of the application. It controls the complete lifecycle of one execution run.

The orchestrator does not contain ETL logic, parsing logic, aggregation logic, or technology-specific code. Its responsibility is only to coordinate execution between the CLI layer, the Query Planners, the reporting system, and the metadata storage layer.

For every execution request, the orchestrator creates a new execution context containing:
- run identifier,
- selected pipeline,
- selected query,
- batch configuration,
- execution timestamps,
- dataset location,
- runtime trackers,
- shared configuration objects.

The orchestrator then selects the appropriate Query Planner implementation based on the chosen execution technology.

Example:
- Hive execution uses HiveQueryPlanner
- Pig execution uses PigQueryPlanner
- MongoDB execution uses MongoQueryPlanner
- MapReduce execution uses MapReduceQueryPlanner

The orchestrator invokes the lifecycle methods:
```java
setup()
execute()
cleanup()

# 3. Query Planners

Separate planner per execution technology.

## all Planners just 4

```text
MapReduceQueryPlanner
PigQueryPlanner
HiveQueryPlanner
MongoQueryPlanner
```

## Responsibilities

### setup()

* initialize technology-specific resources example connections
* initialize DB connections as per need
* initialize Mongo/Hive/Pig clients as per need
* split batches into files

### execute()

For each batch:

* call query pipeline
* measure batch runtime
* record execution metadata

### cleanup()

* drop temp resources
* close clients
* close JDBC resources
* cleanup working directories

# 3. Query Planners

A Query Planner represents one execution technology and acts as the technology-specific execution controller.

There are exactly four Query Planners in the system:
- MapReduceQueryPlanner
- PigQueryPlanner
- HiveQueryPlanner
- MongoQueryPlanner

A Query Planner is responsible for preparing all infrastructure, resources, temporary storage, execution contexts, and batch preparation required by its execution technology.

The planner itself does not contain query aggregation logic. Instead, it selects and invokes the appropriate Query Pipeline implementation for the selected query.

For example:
- HiveQueryPlanner may invoke HiveQuery1Pipeline
- MongoQueryPlanner may invoke MongoQuery3Pipeline

The purpose of separating planners from pipelines is to isolate:
- execution lifecycle management,
- technology setup,
- batching infrastructure,
- connection management

from:
- actual ETL query logic.

This separation prevents duplication of setup and cleanup logic across multiple query implementations.

The planner lifecycle consists of three phases:
- setup()
- execute()
- cleanup()

The setup phase prepares all technology-specific infrastructure required before ETL execution begins. This includes:
- initializing database connections,
- initializing Hadoop configuration,
- initializing Hive sessions,
- initializing MongoDB clients,
- creating temporary working directories,
- preparing batch files,
- validating dataset accessibility,
- preparing metadata collectors.

The execute() phase controls batch execution. For every batch:
- the planner selects the correct Query Pipeline,
- invokes ETL execution,
- tracks runtime,
- stores execution metadata,
- records malformed-row statistics.

If the user selects “Execute All Queries”, the planner sequentially executes all three query pipelines for the same batch.

The cleanup() phase releases all technology-specific resources after execution completes. This includes:
- closing sessions,
- deleting temporary batch files,
- cleaning working directories,
- flushing pending database writes,
- stopping clients.

The Query Planner must never contain:
- CLI logic,
- reporting logic,
- hardcoded query aggregation logic,
- result rendering logic.
# 4. Query Pipelines  

Query Pipelines contain the actual ETL implementation logic of the system.

A Query Pipeline represents:
- one query
- executed using one specific execution technology.

Since the system contains:
- 4 execution technologies
- 3 mandatory queries

the system contains:
- 12 Query Pipeline implementations.

Examples:
- HiveQuery1Pipeline
- PigQuery2Pipeline
- MongoQuery3Pipeline
- MapReduceQuery1Pipeline

A Query Pipeline is responsible for implementing the complete ETL workflow for its query using the native capabilities of its execution technology.

This includes:
- extraction,
- transformation,
- malformed-row handling,
- aggregation,
- relational result loading.

The pipeline is the only layer where actual execution technology logic should exist.

For example:
- MapReduce pipelines use Mappers and Reducers,
- Pig pipelines use Pig Latin scripts,
- Hive pipelines use HiveQL,
- MongoDB pipelines use aggregation pipelines.

Each Query Pipeline processes one batch at a time.

The input to a Query Pipeline includes:
- batch file location,
- execution context,
- database connections,
- metadata trackers,
- runtime collectors.

The output of a Query Pipeline is:
- aggregated query results,
- malformed-row statistics,
- batch execution statistics.

The extraction stage reads raw NASA log records directly from batch files without external preprocessing.

The transformation stage parses raw log lines into structured fields such as:
- host,
- timestamp,
- log_date,
- log_hour,
- HTTP method,
- resource path,
- protocol version,
- status code,
- bytes transferred.

The transformation stage must also:
- convert "-" bytes values into 0,
- detect malformed rows,
- increment malformed counters,
- preserve malformed-row reporting information.

The aggregation stage implements the mandatory query logic using the native aggregation mechanism of the execution technology.

The load stage writes aggregated query results into PostgreSQL/MySQL result tables while associating every row with:
- run_id,
- batch_id,
- pipeline_name.

Query Pipelines must never:
- manage application lifecycle,
- manage CLI interaction,
- perform planner selection,
- render reports.
# 5. Connection Modules

Connection Modules centralize all technology-specific connection and session initialization logic.

The purpose of this layer is to prevent Query Planners and Query Pipelines from manually creating and managing low-level connections repeatedly.

This layer acts as the infrastructure abstraction layer of the system.

Examples include:
- JDBC connection creation,
- MongoDB client initialization,
- Hive JDBC session creation,
- Hadoop job configuration creation,
- Pig execution context initialization.

The Connection Modules are responsible only for:
- creating connections,
- configuring sessions,
- validating connectivity,
- returning reusable client objects.

They must not contain:
- query logic,
- ETL logic,
- orchestration logic,
- reporting logic.

This separation simplifies pipeline implementations and ensures connection management remains centralized and reusable.
# 6. Reporting Module

Reads relational execution metadata DB and prints reports.

## Required Reports

### Query Results

Formatted query output

### Runtime Reports

Per batch:

* runtime
* records processed
* malformed count

### Aggregate Reports

Overall:

* total runtime
* average batch size
* total malformed rows

Required by specification. 

---

# 7. Models

ORM entities or POJOs representing:

* result rows
* metadata rows

## Required Models

```text
Query1Result
Query2Result
Query3Result

RunMetadata
BatchMetadata
```

---
# Batch IDs

Sequential:

```text
1,2,3,4...
```

---

# Final Partial Batch

Must still execute and count.

Required by specification. 

---

# Recommended Batch Strategy

during the setup function of query planner spplit the dataset into files where each file represent each batch 
# Schema Design

## execution_run

Tracks one execution request. use a logical view instead of table. view from batch_execution_metadata. No need to write data into any table.

```sql
CREATE TABLE execution_run (
    run_id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(50),
    query_name VARCHAR(50),
    batch_size INT,
    total_batches INT,
    total_records BIGINT,
    malformed_records BIGINT,
    total_runtime_ms BIGINT,
    average_batch_size DOUBLE PRECISION,
    execution_timestamp TIMESTAMP
);
```

---

## batch_execution_metadata
this would be a table
```sql
CREATE TABLE batch_execution_metadata (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT,
    batch_id INT,
    records_processed BIGINT,
    malformed_records BIGINT,
    batch_runtime_ms BIGINT,
    batch_start_time TIMESTAMP,
    batch_end_time TIMESTAMP
);
```

---


---

# Query Result Tables

## query1_result

```sql
CREATE TABLE query1_result (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT,
    batch_id INT,
    pipeline_name VARCHAR(50),

    log_date DATE,
    status_code INT,
    request_count BIGINT,
    total_bytes BIGINT
);
```

---

## query2_result

```sql
CREATE TABLE query2_result (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT,
    batch_id INT,
    pipeline_name VARCHAR(50),

    resource_path TEXT,
    request_count BIGINT,
    total_bytes BIGINT,
    distinct_host_count BIGINT
);
```

---

## query3_result

```sql
CREATE TABLE query3_result (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT,
    batch_id INT,
    pipeline_name VARCHAR(50),

    log_date DATE,
    log_hour INT,
    error_request_count BIGINT,
    total_request_count BIGINT,
    error_rate DOUBLE PRECISION,
    distinct_error_hosts BIGINT
);
```

---

# Technology-Specific Design

# MapReduce Pipeline

## Recommended Design

### Job 1

Parsing + transformation.

### Job 2

Aggregation.

Use:

* custom Writable
* Combiner
* Reducer

Avoid external preprocessing.

---

# Pig Pipeline

## Recommended Design

* LOAD raw logs
* FOREACH parse
* FILTER malformed
* GROUP
* GENERATE aggregates

Use Pig Latin scripts generated dynamically by Java.

---

# Hive Pipeline

## Recommended Design

* external raw table
* regex serde
* transformation view
* aggregation query

Java executes Hive queries via JDBC.

---

# MongoDB Pipeline

## Recommended Design

Use aggregation pipeline:

* $match
* $project
* $group
* $sort
* $limit

Raw logs inserted batch-wise into working collection.

---

# Execution Flow

```text
User Request
    ↓
Create Run Metadata
    ↓
Initialize Planner
    ↓
Split Dataset Into Batches
    ↓
For each Batch:
    ↓
Extract
Transform
Aggregate
Load Results
Store Metadata
    ↓
Finalize Run Metadata
    ↓
Generate Report
```

---

# Runtime Measurement Rules

Runtime includes:

* reading logs
* parsing
* transformations
* aggregation
* writing results

Runtime excludes:

* dataset download
* installation
* reporting rendering

Required by specification. 

---

# Recommended Package Structure

```text
src/main/java/

app/
orchestrator/

planner/
    mapreduce/
    pig/
    hive/
    mongo/

pipeline/
    query1/
    query2/
    query3/

connection/
model/
reporting/
common/
config/
```

---

# Final Deliverables

The final system must demonstrate:

* CLI based execution
* all 3 queries
* all 4 * 3 pipelines
* batching
* relational loading
* reporting
* runtime metadata
* malformed record reporting


