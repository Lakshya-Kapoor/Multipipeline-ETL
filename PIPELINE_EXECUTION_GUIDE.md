# Pipeline Execution Guide

## Prerequisites

1. **Setup Environment Variables**
   ```bash
   source env_template
   ```
   This loads all required configuration from `env_template`. Verify with:
   ```bash
   echo $MONGO_HOST
   echo $PG_HOST
   echo $HIVE_HOST
   ```

2. **Start Required Services**
   - PostgreSQL: Make sure it's running on `PG_HOST:PG_PORT` with database `PG_DB`
   - MongoDB: Make sure it's running on `MONGO_HOST:MONGO_PORT` with database `MONGO_DATABASE`
   - Hive: Ensure HiveServer2 is running on `HIVE_HOST:HIVE_PORT`

3. **Prepare Data**
   - Place your dataset file in the `data/` directory
   - The application expects NASA access log files

## Building the Project

```bash
cd /home/sahas/PROJECTS/multiPipelineEtl
mvn --no-transfer-progress clean package
```

Output JAR: `target/multiPipelineEtl-1.0-SNAPSHOT.jar`

## Running the Application

```bash
java -jar target/multiPipelineEtl-1.0-SNAPSHOT.jar
```

This starts an interactive CLI menu with these options:
```
1. Execute    → Run a pipeline with specific queries
2. Show Reports → View results from previous executions
3. Exit       → Exit the application
```

## Interactive Pipeline Execution

When you select **Option 1 (Execute)**, you'll be prompted with:

### Step 1: Select Pipeline
```
Select Pipeline: 1.Mongo 2.MapReduce 3.Pig 4.Hive
```
- **1 = MongoDB Pipeline** - Uses MongoDB for data processing
- **2 = MapReduce Pipeline** - Uses Hadoop MapReduce
- **3 = Pig Pipeline** - Uses Apache Pig
- **4 = Hive Pipeline** - Uses Apache Hive

### Step 2: Select Query Type
```
Select Query: 1.Query1 2.Query2 3.Query3 4.Execute All
```
- **1 = Query1** - First analysis query
- **2 = Query2** - Second analysis query
- **3 = Query3** - Third analysis query
- **4 = Execute All** - Run all three queries

### Step 3: Enter Batch Size
```
Batch size: 
```
- Enter batch size (e.g., `1000`)
- Defines how many records to process per batch

### Step 4: Enter Dataset Path
```
Dataset path: 
```
- Provide full path to your dataset file
- Example: `/home/sahas/PROJECTS/multiPipelineEtl/data/sample_logs.txt`

## Execution Flow

After you provide inputs, the application will:

1. **Initialize Schema** (runs once per execution)
   - Creates tables if they don't exist:
     - `batch_execution_metadata` - Tracks batch processing
     - `query1_result` - Results from Query 1
     - `query2_result` - Results from Query 2
     - `query3_result` - Results from Query 3

2. **Execute Pipeline**
   - Process data according to selected pipeline type
   - Run selected query/queries
   - Store results in PostgreSQL

3. **Return Results**
   - Print `Execution complete. run_id=<ID>`
   - Save `run_id` for later reporting

## Viewing Results

After execution, use **Option 2 (Show Reports)**:
```
Enter run_id: <paste run_id from execution>
```

The report displays:

### 1. **Execution Summary** (Aggregated across all batches)
   - Total records processed
   - Total malformed records
   - Total runtime (in seconds/minutes)
   - Malformed rate percentage

### 2. **Query 1 Results** (Top 5 rows, aggregated)
   - Groups by: log_date, status_code
   - Shows: request count, total bytes
   - Aggregates data across all batches

### 3. **Query 2 Results** (Top 5 rows, aggregated)
   - Groups by: resource_path
   - Sorts by: request count (descending)
   - Shows: request count, total bytes, distinct hosts
   - Aggregates data across all batches

### 4. **Query 3 Results** (Top 5 rows, aggregated)
   - Groups by: log_date, log_hour
   - Shows: error requests, total requests, error rate, distinct error hosts
   - Aggregates data across all batches
   - Calculates average error rate across batches

All numbers are formatted with thousands separators for readability.

## Example Execution Sequence

```bash
# Step 1: Setup environment
source env_template

# Step 2: Build project
mvn --no-transfer-progress clean package

# Step 3: Run application
java -jar target/multiPipelineEtl-1.0-SNAPSHOT.jar

# In the interactive menu:
1                                    # Choose Execute
1                                    # Choose MongoDB pipeline
4                                    # Execute all queries
1000                                 # Batch size
/home/sahas/PROJECTS/multiPipelineEtl/data/sample_logs.txt  # Dataset path

# Wait for execution to complete
# You'll see: Execution complete. run_id=<ID>

# View results
2                                    # Choose Show Reports
<paste run_id>                       # Enter the run_id from previous step
```

## Environment Variables Reference

| Variable | Purpose | Example |
|----------|---------|---------|
| `PG_HOST` | PostgreSQL host | `localhost` |
| `PG_PORT` | PostgreSQL port | `5432` |
| `PG_DB` | PostgreSQL database name | `etl2` |
| `PG_USER` | PostgreSQL username | `etl` |
| `PG_PASSWORD` | PostgreSQL password | `pass` |
| `MONGO_HOST` | MongoDB host | `localhost` |
| `MONGO_PORT` | MongoDB port | `27017` |
| `MONGO_DATABASE` | MongoDB database name | `etl` |
| `MONGO_USER` | MongoDB username (optional) | `etl_user` |
| `MONGO_PASSWORD` | MongoDB password (optional) | `pass` |
| `MONGO_AUTH_SOURCE` | MongoDB auth source (optional) | `etl` |
| `HIVE_HOST` | Hive host | `localhost` |
| `HIVE_PORT` | Hive port | `10000` |
| `HIVE_DATABASE` | Hive database | `default` |

## Schema Initialization Details

**Important**: The schema initializer runs **every time** a pipeline executes:
- Uses `CREATE TABLE IF NOT EXISTS` - Safe, won't drop tables
- Creates execution metadata tables
- Creates result tables for each query
- Tables persist between executions (stored in PostgreSQL)

## Troubleshooting

### Missing Environment Variables
```
IllegalStateException: Missing required environment variable: MONGO_HOST
```
**Solution**: Run `source env_template` first

### Connection Refused
```
Connection refused / No connection could be made
```
**Solution**: 
- Check if PostgreSQL/MongoDB/Hive services are running
- Verify host and port in `env_template` are correct

### Dataset File Not Found
```
FileNotFoundException: /path/to/dataset
```
**Solution**: 
- Verify dataset path is correct and accessible
- Ensure file has read permissions

## Configuration Customization

To use different hosts/ports:

1. Create a custom env file (e.g., `env_custom`):
   ```bash
   cp env_template env_custom
   # Edit env_custom with your settings
   ```

2. Load custom environment:
   ```bash
   source env_custom
   ```

3. Run application as normal

## Pipeline Type Details

### MongoDB Pipeline
- Stores intermediate results in MongoDB
- Good for schema-flexible data
- Requires MongoDB connection

### MapReduce Pipeline
- Uses Hadoop distributed processing
- Scalable for large datasets
- Requires Hadoop/HDFS setup

### Pig Pipeline
- Uses Apache Pig for data transformation
- High-level data analysis language
- Requires Pig and Hadoop setup

### Hive Pipeline
- Uses Apache Hive (SQL-like queries)
- Good for structured data
- Requires Hive and Hadoop setup

All pipelines store final results in PostgreSQL.
