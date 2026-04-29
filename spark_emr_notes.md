# Spark & EMR Practical Notes (Production-Level Understanding)

---

## 1. CORE ARCHITECTURE

Cluster
├── Master Node (Driver + Resource Manager)
└── Worker Nodes (Core/Task Nodes)
      ├── Executors
      │     ├── Cores
      │     └── Memory
      └── Tasks (process partitions)

---

## 2. KEY TERMINOLOGY

### Node
Physical machine (EC2 instance)

### Executor
JVM process running Spark tasks

### Core
CPU unit → runs 1 task at a time

### Task
Processes 1 partition

### Partition
Logical chunk of data

---

## 3. YOUR PIPELINE MAPPING

Data:
- CSV: 2 GB
- Parquet: ~700 MB
- Partitions: ~50

Cluster:
- 3 Core Nodes (m5.xlarge)
- ~6 Executors
- 2 cores per executor

Parallelism:
- 6 × 2 = 12 tasks at a time

---

## 4. MEMORY CONCEPTS

### Executor Memory
Total memory assigned to executor (e.g., 4 GB)

### Memory Split
spark.memory.fraction = 0.6

- 60% → Spark usable memory
- 40% → Reserved (JVM, metadata)

Example:
4 GB → 2.4 GB usable

---

### Execution Memory
Used for:
- joins
- shuffle
- transformations

### Storage Memory
Used for:
- caching (not used in your pipeline)

---

## 5. MEMORY OVERHEAD

Extra memory outside JVM heap:
- Python worker (PySpark)
- Serialization
- Native memory

Important:
Executor Memory + Overhead = Total usage

If exceeded → container killed

---

## 6. GARBAGE COLLECTION (GC)

JVM cleans unused objects

Flow:
Data → Objects → Process → Garbage → GC cleans

Problem:
- Large executors → long GC pause
- Job slows down

Solution:
- Use smaller executors (3–4 GB)

---

## 7. SHUFFLE

Data movement across nodes

Occurs in:
- join
- groupBy
- aggregation

Expensive because:
- network + disk + memory

Your optimization:
- Broadcast join → avoids shuffle

---

## 8. DISK SPILL

When memory insufficient:
Data written to disk

Impact:
- Slower execution

Your case:
- Minimal spill due to small data + broadcast

---

## 9. SERIALIZATION

Object ↔ bytes conversion

Used in:
- network transfer
- disk write
- Python ↔ JVM

Slow serialization → slow job

---

## 10. PARTITIONING

Partition size = Total Data / Partitions

Ideal:
100–200 MB

Your case:
~14 MB (acceptable due to light workload)

---

## 11. PARALLELISM

Parallel tasks = Executors × Cores

Your case:
6 executors × 2 cores = 12 tasks

Recommended:
Partitions = 2–4 × cores

Your case:
50 partitions → optimal

---

## 12. MEMORY CALCULATION FORMULA

Executor Memory =
((Partition Size × Cores) / Memory Fraction)
× Spill Adjustment
+ Reserved Memory

---

## 13. YOUR PIPELINE ANALYSIS

### Bronze Job (CSV → Parquet)

- I/O heavy
- No shuffle
- Low memory usage
- CPU + disk bound

### Gold Job (Transformation)

- Broadcast join
- Light memory usage
- No heavy shuffle
- Stable execution

---

## 14. COMMON PROBLEMS

| Issue | Cause |
|------|------|
| Slow job | GC pauses |
| Executor killed | Memory overhead |
| High disk I/O | Spill |
| Network spike | Shuffle |
| Uneven tasks | Data skew |

---

## 15. TUNING STRATEGY

Memory-heavy:
- Increase executor memory

CPU-heavy:
- Increase cores/executors

Shuffle-heavy:
- Tune partitions
- Use AQE

Spill issues:
- Increase memory
- Reduce partition size

---

## 16. GOLDEN RULES

1. Memory depends on partition size × cores
2. Executor memory ≠ usable memory
3. Always include overhead
4. Keep partitions ~3–4× cores
5. Prefer small executors

---

## 17. FINAL INTUITION

Spark Performance =
CPU + Memory + Network + Disk

Your pipeline:
- CPU + I/O bound
- NOT memory bound
- NOT shuffle heavy

---

## 18. INTERVIEW SUMMARY

We designed a Spark pipeline on EMR processing ~2 GB daily data. 
We used small executors (3–4 GB, 2 cores) to maximize parallelism and reduce GC overhead. 
We maintained ~50 partitions for efficient execution and used broadcast joins to avoid shuffle. 
The system was optimized for CPU and I/O rather than memory, ensuring stable and cost-efficient performance.

---

END OF NOTES
