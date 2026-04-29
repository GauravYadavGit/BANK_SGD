# 📘 Spark & EMR Configuration Notes (Production-Level – Financial Pipeline)

---

# 🎯 CONTEXT

This document explains complete Spark + EMR configuration for a financial data pipeline:

- Input: ~2 GB CSV → ~700 MB Parquet
- Output: ~500 MB Gold dataset
- Workload: Transformation + Broadcast Join
- Environment: Shared EMR Cluster

---

# 🏗️ CLUSTER SETUP (SHARED ENVIRONMENT)

## Cluster Configuration

- Master: 1 × m5.xlarge
- Core Nodes: 6 × m5.xlarge
- Task Nodes: Auto-scaled (2–10 spot)

## Key Idea

- Cluster is shared across multiple pipelines
- Resources allocated dynamically via YARN

---

# ⚙️ SPARK JOB CONFIGURATION

## EXECUTOR CONFIG

spark.executor.instances = 6–8  
spark.executor.cores = 2  
spark.executor.memory = 3g  
spark.executor.memoryOverhead = 1g  

Explanation:
Executors are workers. Each runs 2 tasks. Memory split ensures stability.

---

## DRIVER CONFIG

spark.driver.cores = 2  
spark.driver.memory = 2g  

Driver handles DAG and scheduling.

---

## PARTITIONING

spark.sql.shuffle.partitions = 50  
spark.default.parallelism = 50  

Ensures parallelism ~3–4× cores.

---

## AQE

spark.sql.adaptive.enabled = true  
spark.sql.adaptive.coalescePartitions.enabled = true  
spark.sql.adaptive.skewJoin.enabled = true  

Optimizes execution dynamically.

---

## JOIN OPTIMIZATION

Broadcast join used for small exchange rate data → avoids shuffle.

---

## SERIALIZATION

spark.serializer = org.apache.spark.serializer.KryoSerializer  

Faster and memory efficient.

---

## S3 OPTIMIZATION

Fast upload and optimized commit enabled.

---

## FAULT TOLERANCE

Retries enabled to handle transient failures.

---

## LOGGING

Spark event logs stored in S3 for debugging.

---

## TIMEZONE

Asia/Singapore used for financial consistency.

---

# 🧠 PERFORMANCE CONCEPTS

GC → cleans memory  
Memory split → only 60% usable  
Overhead → required for PySpark  
Shuffle → expensive operation  
Spill → disk fallback  
Serialization → impacts speed  

---

# 📊 PIPELINE BEHAVIOR

Bronze:
- I/O heavy

Gold:
- CPU + transformation
- Broadcast join

---

# 🎯 FINAL SUMMARY

Executors: 6–8  
Cores: 2  
Memory: 3–4 GB  
Partitions: 50  

---

# 🧠 INTERVIEW ANSWER

We configured Spark with moderate executors and optimized for parallelism. 
Broadcast joins avoided shuffle and ensured fast execution.

---

# END
