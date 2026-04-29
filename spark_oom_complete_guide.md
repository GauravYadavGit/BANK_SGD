
# 🚀 Spark OOM (Out Of Memory) — Complete Deep Dive Guide

---

## 📌 1. What is OOM?

OOM (Out Of Memory) in Spark occurs when an **executor (JVM process)** does not have enough memory to execute a task.

### 🔴 Typical Errors:
- java.lang.OutOfMemoryError: Java heap space
- ExecutorLostFailure
- Container killed by YARN for exceeding memory limits

---

## 🧠 2. How Spark Uses Memory Internally

Each executor has memory divided into:

Executor Memory
├── Execution Memory (for processing)
├── Storage Memory (for caching)

---

## ⚙️ 3. What Happens During Execution

A Spark task requires memory for:
- Input data
- Shuffle buffers
- Serialization
- Merge buffers
- Sorting
- Output buffers

👉 If required memory > available memory → OOM

---

## 🔥 4. Where OOM Happens Most

### 1. Shuffle (MOST COMMON)
- During repartition, join, groupBy
- Data fetched + merged + sorted

### 2. Wide Transformations
- groupBy
- join
- orderBy

### 3. Data Skew
- One partition becomes huge

### 4. Caching Large Data

### 5. Collecting Data to Driver

---

## 💥 5. Real Root Causes

### ❌ Large Partition Size
Example:
10GB / 4 partitions = 2.5GB per partition

---

### ❌ Data Skew
One partition gets most data

---

### ❌ Too Few Partitions

---

### ❌ Heavy Joins

---

### ❌ Serialization Overhead

---

### ❌ Improper Cache Usage

---

## 📊 6. Real Example (Story)

You have 10GB data and repartition(4)

Each partition = ~2.5GB

During shuffle:
- Data fetched from multiple executors
- Needs buffers + merge + sorting

Actual memory required ≈ 3–4GB

If executor memory is 2GB → OOM

---

## 🧠 7. Key Insight

OOM is NOT about total data size  
It is about:

👉 Partition Size + Memory Overhead

---

## 🚀 8. How to Avoid OOM

---

### ✅ 1. Increase Partitions

Rule:
128MB – 256MB per partition

Example:
10GB → 50 partitions

---

### ✅ 2. Avoid Unnecessary Shuffle

---

### ✅ 3. Handle Data Skew
- Repartition by key
- Salting

---

### ✅ 4. Optimize Joins
- Broadcast small tables

---

### ✅ 5. Tune Configs

spark.sql.shuffle.partitions = 50  
spark.sql.files.maxPartitionBytes = 128MB  

---

### ✅ 6. Avoid collect()

---

### ✅ 7. Use Parquet instead of CSV

---

### ✅ 8. Increase Memory (Last Option)

---

## 📌 9. Debug Using Spark UI

Check:
- Shuffle Read Size
- Task Duration
- Skewed tasks

---

## 🎯 10. Interview Answer (Perfect Version)

OOM in Spark occurs when an executor does not have enough memory to process a task, typically during shuffle operations. It is usually caused by large partition sizes, data skew, or inefficient transformations.

To avoid OOM:
- Increase partition count
- Optimize joins
- Handle skew
- Tune Spark configs
- Avoid large data collection to driver

---

## 🔥 Final Golden Line

👉 OOM is a partition problem, not a data problem

---

## 🧠 Final Mental Model

Map → Split data  
Shuffle → Exchange data  
Reduce → Merge + Process  

---



OOM (Out Of Memory) in Spark occurs when an executor does not have sufficient memory to execute a task.
Spark executors use JVM memory, which is divided into execution memory (for computation) and storage memory (for caching). During execution—especially in shuffle operations—a task requires memory for input data, shuffle buffers, serialization, deserialization, merge buffers, and sorting.
The most common scenario where OOM occurs is during the shuffle reduce phase, because a single task needs to fetch data from multiple executors, merge it, and process it. This creates a memory spike.
The primary root cause is large partition size, where one task ends up processing too much data. Other causes include:
Data skew (one partition significantly larger than others)
Too few partitions
Improper joins (e.g., broadcasting large datasets)
Collecting large data to the driver
Inefficient transformations causing heavy shuffle
For example, if we process 10GB data with only 4 partitions, each partition becomes ~2.5GB. But due to serialization and buffering overhead, the actual memory required may exceed 3–4GB. If the executor does not have enough memory, it results in OOM.
To avoid OOM:
Increase parallelism (maintain ~128–256MB per partition)
Handle data skew using proper partitioning or salting
Optimize joins using broadcast for small datasets
Avoid unnecessary shuffle operations
Tune Spark configurations like spark.sql.shuffle.partitions
Avoid actions like collect() on large datasets
Increase executor memory only as a last resort
Additionally, Spark UI should be used to monitor shuffle read size, task duration, and skewed partitions to identify memory bottlenecks.
🧠 One Killer Line (say this confidently)
OOM in Spark is not caused by total data size, but by how much data a single task processes.
💡 If interviewer pushes deeper
You can add:
OOM typically happens during shuffle read/merge phase because a reduce task needs to aggregate data coming from all map tasks, which increases memory pressure due to buffering and deserialization.
🔥 Final Feedback on You
You’re already thinking like someone who:
understands Spark internals
can debug production issues
not just writing code
Just clean the terminology and structure → you’ll sound very strong in interviews.

