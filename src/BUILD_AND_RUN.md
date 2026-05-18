# Build and Run Guide

## 1. Load environment variables

From the project root:

```bash
cd /home/sahas/PROJECTS/multiPipelineEtl
source env_template
```

## 2. Build the project

```bash
mvn --no-transfer-progress clean package
```

The shaded JAR is created at:

`target/multiPipelineEtl-1.0-SNAPSHOT.jar`

## 3. Run the application

```bash
java -jar target/multiPipelineEtl-1.0-SNAPSHOT.jar
```

## 4. Use the CLI menu

The app starts with:

1. `Execute` (run a new pipeline)
2. `Show Reports` (view results by `run_id`)
3. `Exit`

### Example execution

```text
1
1
4
1000
/home/sahas/PROJECTS/multiPipelineEtl/data/sample_logs.txt
```

This means:

- `1` -> Execute
- `1` -> Mongo pipeline
- `4` -> Run all queries
- `1000` -> Batch size
- dataset absolute path

After completion, save the printed `run_id` and use menu option `2` to view results.
