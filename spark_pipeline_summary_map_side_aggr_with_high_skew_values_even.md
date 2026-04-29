
# 🚀 Spark Pipeline Execution – Complete Mental Model

## 🧠 Cluster Setup
- Executors: 5
- Cores per executor: 2
- Total parallel tasks: 10
- Memory: 4 GB per executor

👉 Only **10 tasks run at the same time**

---

## 📦 Data Source (S3)
- ~10+ GB Parquet (compressed)
- ~320M rows
- Partitioned by `ingestion_date`

---

## 🔍 Stage 1 – Read
- Spark reads only required columns (column pruning)
- Reads only one partition (partition pruning)

👉 Actual data read:
- **2.6 GB**
- **110 partitions created**

Per partition:
- ~25 MB
- ~3M rows

---

## ⚙️ Execution Model
- 110 partitions = 110 tasks
- 10 tasks run in parallel

👉 Total execution waves:
- ~11 waves

---

## 🔵 Stage 2 – Transform (No Shuffle)
- Add currency column
- Add amount column
- Filter data
- Broadcast join (FX)

👉 Still:
- 110 partitions
- 320M rows

---

## 🔥 Stage 3 – Map-Side Aggregation

```python
groupBy("currency")
```

### Data Characteristics
- 169 currencies
- Skew: USD + NULL ≈ 90%

---

### What happens per partition:
- ~3M rows → aggregated locally
- Produces **~150–170 partial rows**

👉 Not final results (partial aggregation)

---

### Total intermediate output:
- 110 × ~169 ≈ **18,590 rows**
- Size: **~596 KB**

---

## 🔄 Shuffle
- Only **18K rows shuffled**
- Not 320M rows

👉 Huge performance gain

---

## 🧠 AQE (Adaptive Query Execution)
Config enabled:
- Adaptive execution
- Partition coalescing

Spark sees:
- Shuffle size = **596 KB**

👉 Decision:
- Reduce partitions from 30 → **1**

---

## 🔵 Stage 5 – Final Aggregation
- 18K partial rows → merged
- Final output: **169 rows**

---

## 🧱 Coalesce(1)
- Already 1 partition due to AQE
- Safe but redundant

---

## 💾 Final Output
- Rows: 169
- Size: 3.8 KB
- Tasks: 1

---

# 🎯 Full Pipeline Flow

S3 (10GB compressed)
↓
Read (2.6GB, 110 partitions)
↓
Transform (no shuffle)
↓
Map-side aggregation (320M → 18K rows)
↓
Shuffle (596 KB only)
↓
AQE (30 → 1 partition)
↓
Final aggregation (18K → 169 rows)
↓
Write (3.8 KB)

---

# 🔥 Key Learnings

## 1. Partitions come from data
- 110 partitions from file splits
- Not from cluster size

## 2. Parallelism is limited by cores
- Only 10 tasks run simultaneously

## 3. Map-side aggregation is critical
- Reduces data early (320M → 18K)

## 4. Shuffle is optimized
- Only small aggregated data moved

## 5. AQE improves performance
- Dynamically reduces partitions

## 6. Skew impact avoided
- Early aggregation prevents skew issues

---

# 💡 One-Line Summary

Spark optimized the pipeline by aggregating data locally before shuffle, reducing 320M rows to 18K, and then using AQE to execute the final stage efficiently in a single task.
