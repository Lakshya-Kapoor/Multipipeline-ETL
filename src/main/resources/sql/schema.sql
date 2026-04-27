CREATE TABLE IF NOT EXISTS query1_results (
    id BIGSERIAL PRIMARY KEY,
    execution_choice VARCHAR(50) NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,
    batch_id INTEGER NOT NULL,
    log_date DATE NOT NULL,
    status_code INTEGER NOT NULL,
    request_count BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL,
    executed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS query2_results (
    id BIGSERIAL PRIMARY KEY,
    execution_choice VARCHAR(50) NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,
    batch_id INTEGER NOT NULL,
    resource_path TEXT NOT NULL,
    request_count BIGINT NOT NULL,
    total_bytes BIGINT NOT NULL,
    distinct_host_count BIGINT NOT NULL,
    executed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS query3_results (
    id BIGSERIAL PRIMARY KEY,
    execution_choice VARCHAR(50) NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,
    batch_id INTEGER NOT NULL,
    log_date DATE NOT NULL,
    log_hour INTEGER NOT NULL CHECK (log_hour BETWEEN 0 AND 23),
    error_request_count BIGINT NOT NULL,
    total_request_count BIGINT NOT NULL,
    error_rate NUMERIC(12, 6) NOT NULL,
    distinct_error_hosts BIGINT NOT NULL,
    executed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_metadata (
    id BIGSERIAL PRIMARY KEY,
    execution_choice VARCHAR(50) NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,
    query_number INTEGER NOT NULL,
    query_name VARCHAR(100) NOT NULL,
    batch_id INTEGER,
    batch_size INTEGER NOT NULL,
    records_processed BIGINT NOT NULL,
    malformed_records BIGINT NOT NULL,
    runtime_millis BIGINT NOT NULL,
    average_batch_size NUMERIC(14, 4) NOT NULL,
    executed_at TIMESTAMP NOT NULL
);
