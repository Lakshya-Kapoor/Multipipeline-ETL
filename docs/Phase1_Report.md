# Phase 1 Report: Multi-Pipeline ETL and Reporting Framework

## 1. Proposed Architecture

The system is designed as a **single controller-driven ETL tool** with a pluggable execution backend.  
An end user selects one pipeline (**Pig, MapReduce, MongoDB, Hive**) from the CLI, and the orchestrator runs the same logical workflow for the selected backend.

### Core components

1. **Execution Controller (UI layer)**  
   Collects:
   - execution choice (`PIG`, `MAPREDUCE`, `MONGODB`, `HIVE`)
   - number of batches

2. **Pipeline Orchestrator (application layer)**  
   - validates request
   - loads runtime configuration (MongoDB + PostgreSQL)
   - builds a shared `QueryExecutionContext`
   - invokes pipeline implementations

3. **Pipeline Contract Layer (abstraction layer)**  
   Common interfaces:
   - `Query1Pipeline`
   - `Query2Pipeline`
   - `Query3Pipeline`

   This enforces a consistent lifecycle across backends and supports future implementations without changing controller/orchestrator logic.

4. **Pipeline Implementations (execution layer)**  
   Current working prototype includes:
   - **MongoDB Query 1 pipeline** (`MongoDbQuery1Pipeline`)

5. **Relational Reporting Layer (PostgreSQL)**  
   Aggregated query outputs and execution metadata are stored in PostgreSQL tables and consumed by `ReportingModule`.

---

## 2. Overall Tool Design

The tool follows this end-to-end flow:

1. User chooses execution backend and number of batches.
2. Orchestrator initializes DB connections and execution context.
3. Selected query pipeline processes data batch-by-batch.
4. Aggregated results are loaded into PostgreSQL result tables.
5. Execution metadata (runtime, records processed, malformed records, average batch size, etc.) is written to metadata table.
6. Reporting module reads relational tables and prints results in a structured format.

### Design principles used

- **Separation of concerns**: UI, orchestration, processing, storage, and reporting are separate modules.
- **Backend-switching architecture**: same controller/orchestrator with interchangeable pipeline implementation.
- **Batch-aware execution**: processing and metrics are explicitly batch-indexed.
- **Comparative-ready reporting**: schema includes execution choice and pipeline name so runs can be compared later.

---

## 3. Parsing Strategy

For the NASA HTTP logs, the current parser (in `MongoDbQuery1Pipeline`) uses a regex-based extraction strategy:

- Match each raw log line against a strict log pattern.
- Extract:
  - timestamp token
  - HTTP status code
  - bytes transferred
- Parse timestamp into date (`log_date`) using:
  - NASA format: `dd/MMM/yyyy:HH:mm:ss Z`
  - fallback format: `EEE MMM dd HH:mm:ss yyyy`
- Normalize bytes:
  - `"-"` is converted to `0`
- Treat invalid lines as malformed:
  - malformed count is tracked and stored in execution metadata
  - malformed records are not silently ignored in metrics

This Phase 1 parser strategy demonstrates how each backend can implement equivalent parsing rules while preserving consistency in output semantics.

---

## 4. ETL Workflow (Current Prototype)

The implemented pipeline follows a 3-stage ETL loop per batch:

1. **Extract + Parse**
   - Read only the selected batch window from input dataset.
   - Parse lines into structured records.
   - Insert parsed records into MongoDB staging collection (`query1_stage`) with `batch_id`.

2. **Transform / Aggregate**
   - Run MongoDB aggregation grouped by:
     - `batch_id`
     - `log_date`
     - `status_code`
   - Compute:
     - `request_count`
     - `total_bytes`
   - Store intermediate aggregate in MongoDB aggregation collection (`query1_agg`).

3. **Load**
   - Read aggregated batch result from MongoDB.
   - Insert into PostgreSQL `query1_results`.
   - Write per-batch and overall run metadata into `execution_metadata`.

### Runtime and batching metadata captured

- batch id
- batch size
- records processed
- malformed records
- runtime in milliseconds
- average batch size
- execution timestamp

---

## 5. Relational Reporting Database Schema

The reporting schema (in `src/main/resources/sql/schema.sql`) includes four tables:

### `query1_results`
Stores Daily Traffic Summary output:

- `execution_choice`
- `pipeline_name`
- `batch_id`
- `log_date`
- `status_code`
- `request_count`
- `total_bytes`
- `executed_at`

### `query2_results`
Prepared for Top Requested Resources output:

- `execution_choice`, `pipeline_name`, `batch_id`
- `resource_path`
- `request_count`
- `total_bytes`
- `distinct_host_count`
- `executed_at`

### `query3_results`
Prepared for Hourly Error Analysis output:

- `execution_choice`, `pipeline_name`, `batch_id`
- `log_date`, `log_hour`
- `error_request_count`
- `total_request_count`
- `error_rate`
- `distinct_error_hosts`
- `executed_at`

### `execution_metadata`
Stores operational run and batch metrics:

- `execution_choice`
- `pipeline_name`
- `query_number`
- `query_name`
- `batch_id` (nullable for overall run row)
- `batch_size`
- `records_processed`
- `malformed_records`
- `runtime_millis`
- `average_batch_size`
- `executed_at`

This schema supports both **result reporting** and **cross-pipeline comparative analysis**.

---

## 6. Phase 1 Prototype Status

- Execution-choice interface is implemented.
- Orchestrator and shared execution context are implemented.
- Query contracts for all three mandatory queries are defined.
- MongoDB-based Query 1 pipeline is implemented and integrated.
- PostgreSQL reporting schema and reporting module are implemented.

This satisfies the Phase 1 expectation of presenting architecture and demonstrating a working prototype with at least one pipeline, while keeping the design extensible for full Phase 2 completion.
