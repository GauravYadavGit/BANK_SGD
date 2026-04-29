# 🧩 Spark Debugging Story — End-to-End Learning

## 🚀 Context

We built a Spark pipeline:

Bronze (S3 ~3GB, 120M rows)  
→ Join with FX (~16MB, 172 rows)  
→ Transform  
→ Repartition  
→ Write to Silver (Parquet)

---

## ❌ Initial Problem

We used:
```python
df.count()
```

This caused:
- Full data scan
- DAG recomputation
- Multiple Spark jobs
- Extra S3 reads

👉 Fix: Removed `.count()`

---

## 🔍 Clean Execution (After Fix)

Now Spark executed:
- Minimal jobs
- Single DAG
- Proper repartition (8 files)

---

## 🧠 Deep Dive — What Actually Happened

### Step 1: Read Data
- 3GB transactions (120M rows)
- 16MB FX data

### Step 2: Broadcast Join
- FX table broadcasted
- No shuffle during join
- Output: ~55M rows

---

### Step 3: Shuffle (Exchange)

```text
55M rows → repartition(8)
→ shuffle size: 3.9GB
```

Each partition ≈ 500MB

---

### Step 4: Sort (Unexpected)

Even though we didn’t write sort, Spark added:

👉 Internal sort (for shuffle/write optimization)

---

### Step 5: Spill (Major Finding)

```text
Input: 3.9GB
Spill: 13GB
```

---

## 🧩 Why Spill Happened (Layman Explanation)

Imagine sorting books:

- You have 500 books
- Table fits only 100

So:
1. Sort 100 → keep aside
2. Sort next 100 → keep aside
3. Merge all

👉 This is exactly Spark spill

---

## 🧠 Technical Reason

- Each task had ~500MB data
- Memory < required for sorting
- Spark:
  → sorted chunks
  → spilled to disk
  → merged later

---

## 🔥 Key Insight

```text
Spill ≠ data size
Spill = cumulative disk IO
```

---

## 🚨 Why This Is Dangerous

At scale:

```text
3GB → 13GB spill
10GB → 40–60GB spill
```

---

## 🔧 Fix Strategy

### 1. Increase partitions
```python
df.repartition(16)
```

### 2. Use AQE
```bash
spark.sql.adaptive.enabled=true
```

### 3. Advisory partition size
```bash
spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB
```

---

## 🧠 What Advisory Partition Size Does

```text
3.9GB / 64MB ≈ 60 partitions
```

Instead of 8 large partitions → many small ones

👉 Each task handles less data → less spill

---

## 🔥 Final Pipeline (Optimized)

```text
Read → Broadcast Join → Shuffle → Small partitions → Minimal spill → Write
```

---

## 🧠 Key Learnings

- Actions trigger full execution
- Broadcast join avoids shuffle
- Shuffle is expected
- Sort is memory-heavy
- Spill is real bottleneck
- Partition size controls performance
- AQE dynamically optimizes execution

---

## 🎯 Final Takeaway

```text
Large partitions → memory pressure → spill → slow job
Small partitions → efficient execution → faster job
```

---

## 🚀 Level Up Achieved

You now understand:
- Spark DAG
- Shuffle mechanics
- Spill behavior
- Memory vs execution
- AQE optimization

👉 This is real production-level Spark understanding.
