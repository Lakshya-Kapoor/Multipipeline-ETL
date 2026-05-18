# Reporting Module - Enhanced Features

## Overview
The ReportService has been enhanced to provide aggregated summaries across batches and display only the top 5 result rows in a clean, formatted table.

## Features

### 1. Aggregated Execution Summary
Instead of showing per-batch details, displays consolidated metrics:
- **Total Records Processed**: Sum across all batches with comma formatting
- **Total Malformed Records**: Sum across all batches
- **Total Runtime**: Formatted as human-readable (ms/s/m)
- **Malformed Rate**: Percentage of malformed records

### 2. Query 1 Results (Aggregated)
```
===== QUERY 1 RESULTS (Top 5 Rows - Aggregated) =====
LOG_DATE     | STATUS | REQUEST_COUNT   | TOTAL_BYTES
-------------------------------------------------------
2024-01-15   |    200 |         250,000 |    15,000,000
2024-01-15   |    404 |          10,500 |       500,000
2024-01-16   |    200 |         380,000 |    25,000,000
2024-01-16   |    301 |           5,200 |       250,000
2024-01-17   |    200 |         420,000 |    28,500,000
```
- **Grouping**: Aggregates by log_date and status_code
- **Aggregation**: SUM of request_count and total_bytes across all batches
- **Limit**: Shows top 5 unique combinations

### 3. Query 2 Results (Aggregated)
```
===== QUERY 2 RESULTS (Top 5 Rows - Aggregated) =====
RESOURCE_PATH                  | REQUEST_COUNT   | TOTAL_BYTES     | DISTINCT_HOSTS
-------------------------------------------------------------------------------------------
/images/sample.gif             |         250,000 |    10,000,000   |          1,250
/index.html                    |         200,000 |     8,500,000   |          1,100
/icons/icon.gif                |         150,000 |     5,200,000   |            850
/api/data                      |          80,000 |     3,200,000   |            420
/resources/style.css           |          70,000 |     2,100,000   |            380
```
- **Grouping**: Aggregates by resource_path
- **Sorting**: By request_count in descending order (top resources first)
- **Aggregation**: SUM of request_count, total_bytes, distinct_host_count
- **Path Truncation**: Long paths truncated to 30 chars with "..."

### 4. Query 3 Results (Aggregated)
```
===== QUERY 3 RESULTS (Top 5 Rows - Aggregated) =====
LOG_DATE     | HOUR  | ERROR_REQ     | TOTAL_REQ     | ERROR_RATE | ERROR_HOSTS
-------------------------------------------------------------------------------------------
2024-01-15   |    00 |         2,500 |       150,000 |      1.67% |        500
2024-01-15   |    01 |         1,800 |       120,000 |      1.50% |        420
2024-01-15   |    02 |         3,200 |       180,000 |      1.78% |        650
2024-01-15   |    03 |         2,100 |       145,000 |      1.45% |        480
2024-01-15   |    04 |         1,950 |       135,000 |      1.44% |        450
```
- **Grouping**: Aggregates by log_date and log_hour
- **Aggregation**: SUM of error counts and total counts, AVG of error_rate
- **Formatting**: Error rate displayed as percentage with 2 decimal places
- **Sorting**: By log_date and log_hour in ascending order

## Formatting Features

### Number Formatting
- Large numbers display with thousand separators (e.g., `1,234,567`)
- Decimal numbers show 2 decimal places (e.g., `1.67%`)

### Table Layout
- Fixed-width columns for alignment
- Separator lines for readability
- Headers clearly labeled

### Runtime Display
- Milliseconds: `500ms`
- Seconds: `45s`
- Minutes+Seconds: `2m 30s`

## SQL Aggregation Strategy

### Query 1
```sql
SELECT log_date, status_code, SUM(request_count), SUM(total_bytes)
FROM query1_result 
WHERE run_id = ?
GROUP BY log_date, status_code
ORDER BY log_date, status_code
LIMIT 5
```

### Query 2
```sql
SELECT resource_path, SUM(request_count), SUM(total_bytes), SUM(distinct_host_count)
FROM query2_result 
WHERE run_id = ?
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 5
```

### Query 3
```sql
SELECT log_date, log_hour, SUM(error_request_count), SUM(total_request_count), 
       AVG(error_rate), SUM(distinct_error_hosts)
FROM query3_result 
WHERE run_id = ?
GROUP BY log_date, log_hour
ORDER BY log_date, log_hour
LIMIT 5
```

## Benefits

✅ **Cleaner Output**: See summary stats at a glance, no per-batch noise
✅ **Top Results**: Shows most important 5 rows for each query
✅ **Aggregated**: All batch data combined for holistic view
✅ **Readable Format**: Comma-separated numbers, aligned columns
✅ **Error Rates**: Automatic percentage calculations
✅ **Flexible Sorting**: Each query optimally sorted (e.g., Query 2 by popularity)

## Example Complete Report

When you run `Show Reports` and enter a run_id, you'll see:

```
===== EXECUTION SUMMARY: Run 1234567 =====
Total Records Processed:  1,250,000
Total Malformed Records:  15,200
Total Runtime:            2m 34s
Malformed Rate:           1.22%

===== QUERY 1 RESULTS (Top 5 Rows - Aggregated) =====
[formatted table]

===== QUERY 2 RESULTS (Top 5 Rows - Aggregated) =====
[formatted table]

===== QUERY 3 RESULTS (Top 5 Rows - Aggregated) =====
[formatted table]
```
