
# Spark Shuffle Explained (Layman + Real Execution View)

## Setup Example
- 2 Executors
- 2 Cores each → 4 parallel tasks
- Repartition(8)

---

## Big Picture

Stage 1 (Map Side):
- 4 tasks run
- Each reads data and splits into 8 partitions
- Writes shuffle files

Stage 2 (Reduce Side):
- 8 tasks created (1 per partition)
- Run in waves (only 4 cores available)

---

## Stage 1: Map Side

Each task:
1. Reads its chunk of data
2. Decides which partition each row belongs to (1–8)
3. Creates 8 buffers (P1–P8)
4. Serializes data (convert to bytes)
5. If memory fills → spills to disk
6. Writes ONE shuffle file (with 8 segments inside)

Important:
Each task contributes data to ALL partitions.

---

## Shuffle Concept

Shuffle = 
Write locally → Fetch remotely → Merge

Data is NOT sent directly memory-to-memory.
It is first written to disk, then fetched.

---

## Stage 2: Reduce Side

8 new tasks are created:
- Each task handles one partition

Example: Task P3

Steps:
1. Fetch partition 3 data from all shuffle files
2. Read from disk + network
3. Deserialize (bytes → rows)
4. Merge all pieces
5. Sort (if needed)

Final:
Partition 3 = data from ALL map tasks

---

## Key Understanding

Each partition is built like:
Partition 3 =
  Task A contribution +
  Task B contribution +
  Task C contribution +
  Task D contribution

---

## Why OOM Happens

Example:
10GB data with repartition(4)

Partition size ≈ 2.5GB

Memory needed:
- Raw data
- Network buffers
- Merge buffers
- Serialization overhead

Total ≈ 3–4GB

If executor memory < this → OOM

---

## Safe Partition Rule

Always aim:
128MB – 256MB per partition

Example:
10GB → ~50 partitions

---

## Final Mental Model

Map Stage:
  Split data into all partitions

Shuffle:
  Exchange data across executors

Reduce Stage:
  Collect + merge + process

---

## One Line Summary

Shuffle means:
Every task sends a piece of data to every partition.

