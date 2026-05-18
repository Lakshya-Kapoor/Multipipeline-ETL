# Quick Start Guide

## Setup (First Time Only)

```bash
cd /home/sahas/PROJECTS/multiPipelineEtl

# 1. Load environment variables
source env_template

# 2. Build the project
mvn --no-transfer-progress clean package
```

## Running Pipelines

```bash
# Start the interactive application
java -jar target/multiPipelineEtl-1.0-SNAPSHOT.jar
```

### Menu Options:
- **Option 1**: Execute a new pipeline
- **Option 2**: View results from a previous run
- **Option 3**: Exit

## Executing a Pipeline

When you select **Option 1**, follow these prompts:

```
1                                          # Choose "Execute"
1                                          # Select Pipeline (1=Mongo, 2=MapReduce, 3=Pig, 4=Hive)
4                                          # Select Query (1=Q1, 2=Q2, 3=Q3, 4=All)
1000                                       # Enter batch size
/path/to/data/sample_logs.txt             # Enter dataset path
```

Output:
```
Execution complete. run_id=1234567890
```

## Viewing Results

```
2                                          # Choose "Show Reports"
1234567890                                 # Paste the run_id from execution
```

This displays:
- **Execution Summary**: Total records, malformed count, runtime, error rate
- **Query 1 Results**: Top 5 rows (by date/status) - aggregated
- **Query 2 Results**: Top 5 rows (by popularity) - aggregated
- **Query 3 Results**: Top 5 rows (by date/hour) - aggregated

## File Structure

```
project/
├── env_template                           # Configuration file (required)
├── PIPELINE_EXECUTION_GUIDE.md           # Detailed execution guide
├── REPORTING_FEATURES.md                 # Reporting features documentation
├── pom.xml                               # Maven build configuration
├── src/main/java/                        # Source code
│   └── com/example/multipipelineetl/
│       ├── app/App.java                  # Main entry point
│       ├── connection/                   # Database connections
│       ├── reporting/ReportService.java  # Reporting module
│       ├── persistence/                  # Database operations
│       └── pipeline/                     # Pipeline implementations
├── target/
│   └── multiPipelineEtl-1.0-SNAPSHOT.jar # Compiled JAR
└── data/                                 # Place datasets here
```

## Configuration

Edit `env_template` to change:
- PostgreSQL: `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`
- MongoDB: `MONGO_HOST`, `MONGO_PORT`, `MONGO_DATABASE`, `MONGO_USER`, `MONGO_PASSWORD`
- Hive: `HIVE_HOST`, `HIVE_PORT`, `HIVE_DATABASE`
- Tool paths: `JAVA_HOME`, `HIVE_HOME`, `HADOOP_HOME`, `PIG_HOME`

Then reload:
```bash
source env_template
```

## Troubleshooting

| Error | Solution |
|-------|----------|
| `Missing required environment variable` | Run `source env_template` |
| `Connection refused` | Verify PostgreSQL/MongoDB/Hive are running and configured correctly |
| `FileNotFoundException` | Ensure dataset path is absolute and file exists |
| `OutOfMemoryError` | Increase Java heap: `java -Xmx4g -jar ...` |

## Common Tasks

### Run Mongo Pipeline with all queries
```
1   # Execute
1   # Mongo
4   # All queries
1000 # Batch size
/path/to/data.txt
```

### Run MapReduce Query 3 only
```
1   # Execute
2   # MapReduce
3   # Query 3
1000
/path/to/data.txt
```

### View latest report
```
2   # Show Reports
<paste run_id>
```

## Pipeline Comparison

| Pipeline | Speed | Scalability | Requires | Best For |
|----------|-------|-------------|----------|----------|
| MongoDB | Fast | Medium | MongoDB running | Dev/testing, flexible schema |
| MapReduce | Slow | High | Hadoop cluster | Very large datasets |
| Pig | Medium | High | Hadoop + Pig | ETL transformations |
| Hive | Medium | High | Hadoop + Hive | SQL-like queries |

## Tips

✅ Always source env_template before running
✅ Use absolute paths for dataset files
✅ Save run_id immediately after execution
✅ Check PostgreSQL/MongoDB logs if connection fails
✅ Use batch size 500-2000 for good balance
✅ Run with all 3 queries first to see full results

## Full Documentation

- **PIPELINE_EXECUTION_GUIDE.md**: Complete step-by-step instructions
- **REPORTING_FEATURES.md**: Detailed reporting module features
