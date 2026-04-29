Below is a complete, full-length, copy-paste-ready .md document that includes:

Business + engineering narrative
Exact numbers (320M, 10GB, 80 partitions, etc.)

Airflow orchestration
Spark internals (map-side aggregation, shuffle)
Cluster + executor reasoning
Partition math
Debugging learnings
Configs
Diagrams
Code snippets


🏦 SGD Financial Data Pipeline

Production-Grade End-to-End System Documentation

📖 0. Pipeline Story (Full Narrative)
Every day, the organization receives approximately 320 million financial transactions (~10GB CSV data) from upstream banking systems. These systems are owned by a separate ingestion team responsible for collecting and delivering the data into the data lake (S3 raw layer).

Each transaction represents either:
A deposit OR
A withdrawal
Both cannot exist simultaneously for a single record.
Similarly:
If deposit_currency is present → withdrawal_currency is null
If withdrawal_currency is present → deposit_currency is null
This creates a mutually exclusive structure for both amount and currency columns.

Business Requirement
All financial reporting must be standardized into SGD (Singapore Dollar).
However, financial correctness requires:
Transactions (T-1) MUST use Exchange Rates (T-1)
This ensures:
Financial accuracy
Audit compliance
Deterministic reporting
No dependency on real-time APIs
System Design Decision
Instead of relying on live exchange rates:
We use historical FX API
We fetch T-1 rates explicitly
This removes runtime dependency risks
Pipeline Design
The system is implemented using three independent but orchestrated pipelines:
FX Ingestion Pipeline
CSV → Parquet Conversion Pipeline
Spark Aggregation Pipeline


All are orchestrated via Airflow DAG
🎯 1. Business Overview
Objective
Process:
320 million records daily
~10GB CSV data
Convert into:
SGD standardized values
Deliver:
Aggregated financial metrics for downstream teams


End Users
Stakeholder Usage
Finance Team    Reconciliation
BI Team Dashboards
Risk Team   Exposure analysis
Compliance  Regulatory reporting


Business Context
Region: APAC (Singapore)
Currency: SGD
Processing Model: T-1


📊 2. Data Sources
Transaction Data
Attribute   Details
Source  Internal Banking Systems
Ownership   Ingestion Team
Format  CSV
Volume  10GB (~320M records)
Delivery    S3 Raw Layer

Data Ownership Flow

Banking Systems
      ↓
Ingestion Team
      ↓
S3 Raw Layer
      ↓
Our Pipeline

Exchange Rate Data
Attribute   Details
Source  External API
Type    Historical
Size    ~200–500KB
Availability    Always


🏗️ 3. Architecture
                +----------------------+
                |   FX API (External)  |
                +----------+-----------+
                           |
                           ↓
                Pipeline 1 (FX Ingestion)
                           ↓
                   S3 (FX Layer)

Raw CSV (S3)
    ↓
Pipeline 2 (CSV → Parquet)
    ↓
Bronze Layer (~3GB)
    ↓
Pipeline 3 (Spark Aggregation)
    ↓
Silver Layer (~KB level output)
    ↓
BI / Analytics / Reporting
🔄 4. Airflow Orchestration


DAG Flow
START
  ↓
fetch_fx_data
  ↓
wait_for_csv_data
  ↓
csv_to_parquet
  ↓
spark_aggregation
  ↓
END

Execution Logic
FX ingestion → no dependency → runs immediately
CSV pipeline → waits for ingestion team
Aggregation → runs after both complete

Airflow Code
wait_for_raw = S3KeySensor(
    task_id="wait_for_raw_csv",
    bucket_name="data-lake",
    bucket_key="raw/transactions/date={{ ds }}/_SUCCESS",
    poke_interval=300,
    timeout=3600,
)

run_transform = EmrAddStepsOperator(
    task_id="run_transformation",
    job_flow_id=CLUSTER_ID,
    steps=[{
        "Name": "Spark Job",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://bucket/main.py",
                "{{ ds }}",
            ],
        },
    }],
)


⏱️ 5. Execution Timeline (SGT)
Time    Step
05:00   DAG start
05:02   FX ready
05:30   CSV arrives
05:40   Parquet ready
05:55   Aggregation done
06:00   Output ready


🟦 6. Pipeline 1 — FX Ingestion
GET /historical/{T-1}.json
Output:
s3://data-lake/exchange_rates/year=YYYY/month=MM/day=DD/


🟫 7. Pipeline 2 — CSV → Parquet
Input
10GB CSV
320M rows
Processing
Read CSV
Apply schema
Convert to Parquet
Partition by ingestion_date
Output
~3GB Parquet
Benefit
CSV Parquet
10GB    3GB
Row-based   Columnar
Slow    Optimized


🟩 8. Pipeline 3 — Spark Aggregation
Flow
Read → Validate → Add Columns → Join → Convert → Aggregate → Write
Code
df = df.withColumn(
    "currency", coalesce(col("withdrawal_currency"), col("deposit_currency"))
)

df = df.withColumn(
    "amount", coalesce(col("withdrawal_amt"), col("deposit_amt"))
)

df = df.join(broadcast(fx_df), on="currency", how="left")

df = df.withColumn("amount_sgd", col("amount") * col("rate_to_sgd"))

agg_df = df.groupBy("currency").agg(
    count("*").alias("txn_count"),
    sum("amount_sgd").alias("total_value_sgd")
)


⚡ 9. Spark Execution Internals
Input
320M rows
3GB Parquet
Execution Behavior
320M rows
   ↓
Map-side aggregation
   ↓
~18K rows
   ↓
Shuffle (~596KB)
   ↓
Final aggregation
   ↓
~169 rows
Key Insight
The pipeline is efficient because:
Aggregation happens before shuffle
Only aggregated data is shuffled


🧠 10. Partitioning Strategy
Calculation
10GB / 128MB ≈ 80 partitions
Cluster Setup
Component   Value
Master  1
Workers 3
Executors   6
Cores   2
Parallel Tasks  ~12
Execution Waves
80 partitions / 12 tasks ≈ 7 waves


🚀 11. Optimization Strategy
Technique   Impact
Broadcast Join  No shuffle
Map-side aggregation    320M → 18K
AQE Partition reduction
Proper partitioning Balanced load


⚠️ 12. Debugging Learnings
Issue
df.count()
→ Triggered full recomputation
Spill
3.9GB shuffle → 13GB spill
Fix
Increased partitions
Enabled AQE
Reduced partition size


📦 13. Final Output
Schema
currency
txn_count
total_value_sgd
ingestion_date
Example
USD | 120M | 3.5M SGD | 2026-04-15
EUR | 80M  | 2.1M SGD | 2026-04-15


⚙️ 14. Spark Configuration
CSV → Parquet
spark.executor.instances=6
spark.executor.cores=2
spark.executor.memory=4g
spark.executor.memoryOverhead=1g

spark.sql.shuffle.partitions=80
spark.default.parallelism=80

spark.sql.files.maxPartitionBytes=128MB
spark.sql.adaptive.enabled=true


Aggregation Job
spark.executor.instances=6
spark.executor.cores=2
spark.executor.memory=4g
spark.executor.memoryOverhead=1g

spark.sql.shuffle.partitions=80
spark.default.parallelism=80

spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB

spark.sql.autoBroadcastJoinThreshold=50MB


🎯 15. Final Summary
This pipeline processes 320 million transactions (~10GB daily), converts them into SGD using historical exchange rates, and produces aggregated outputs.
Key strengths:
Deterministic T-1 processing
Efficient Spark execution
Minimal shuffle via map-side aggregation
Scalable Airflow orchestration
Completion within 1-hour SLA
