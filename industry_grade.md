# 🏦 Production-Grade Financial Data Pipeline (Real-World Simulation)

---

# 🎯 Objective

Design a **production-grade data pipeline** to:

* Ingest **multi-currency transaction data**
* Fetch **exchange rates**
* Convert all transactions into **SGD**
* Produce **analytics-ready datasets**

---

# 🌍 Domain Context

* **Domain**: Financial Services / Banking / Payments
* **Region**: APAC (Singapore-centric reporting)
* **Currency Standard**: SGD
* **Use Case**:

  * Financial reporting
  * Reconciliation
  * BI dashboards

---

# 📊 Data Characteristics

## 🟫 Transaction Data

* Source: Internal banking/payment systems
* Nature: Batch (end-of-day aggregated)
* Volume:

  * ~10 million records/day
  * ~3–6 GB/day

### Schema

```
transaction_id
account_no (PII)
amount
currency
type (deposit / withdrawal)
value_date
```

---

## 🟦 Exchange Rate Data

* Source: External API (e.g., Open Exchange Rates)
* Nature: Daily reference rates
* Size: Small dataset (few KBs)

### Schema

```
date
base_currency (SGD)
target_currency
rate_to_sgd
```

---

# ⏱️ Data Arrival Pattern (REAL INDUSTRY)

## 🟫 Transaction Data (T-1)

| Event           | Time (SGT)    |
| --------------- | ------------- |
| Day ends        | 23:59         |
| Aggregation     | 00:00 – 03:00 |
| Available in S3 | 03:00 – 05:00 |

### Key Rule

```
On Date T → You receive data for (T-1)
```

---

## 🟦 Exchange Rate Data

### Important Insight

* We DO NOT depend on today's API availability
* We fetch **historical rates**

### API Call

```
GET /historical/{T-1}.json
```

---

# ⚖️ Business Logic (CRITICAL)

## ✅ Correct Alignment

```
Transactions (T-1)
+
Exchange Rate (T-1)
```

---

## ❌ Incorrect (Avoid)

```
Transactions (T-1)
+
Exchange Rate (T) ❌
```

---

## 💰 Conversion Formula

```
SGD = amount / rate_to_sgd
```

---

# 🏗️ Architecture Overview

```
Pipeline 1: Exchange Rate Ingestion (API → S3)
Pipeline 2: CSV → Parquet (Bronze Layer)
Pipeline 3: Spark Transformation (EMR → Gold Layer)
```

---

# 🟦 Pipeline 1: Exchange Rate Ingestion

## Purpose

* Fetch historical exchange rates (T-1)

## Output

```
s3://data-lake/exchange_rates/date=YYYY-MM-DD/
```

## Key Features

* Idempotent writes
* Retry logic
* Small dataset optimization

---

# 🟫 Pipeline 2: CSV → Bronze (Standardization)

## Purpose

Convert raw CSV → Parquet

## Input

```
s3://data-lake/raw/transactions/date=YYYY-MM-DD/
```

## Output

```
s3://data-lake/bronze/transactions/value_date=YYYY-MM-DD/
```

## Key Features

* Partitioning by `value_date`
* Schema normalization
* Idempotent overwrite

## Why This Layer?

* CSV is inefficient
* Enables partition pruning
* Improves Spark performance

---

# 🟩 Pipeline 3: Spark Transformation (EMR)

## Purpose

Convert all transactions into SGD

---

## Flow

```
Read → Validate → Split → Mask → Join → Transform → Write
```

---

## Key Steps

### 1. Read

* Bronze transactions
* Exchange rates

---

### 2. Validation

Split data:

```
valid_df
reject_df
```

---

### 3. PII Masking

```
account_no → hashed
```

---

### 4. Broadcast Join

* Exchange rates are small → broadcast
* Avoids shuffle

---

### 5. Transformation Logic

| Type       | Currency Used       |
| ---------- | ------------------- |
| Deposit    | deposit_currency    |
| Withdrawal | withdrawal_currency |

---

### 6. Conversion

```
SGD = amount / rate_to_sgd
```

---

### 7. Output

```
s3://data-lake/gold/transactions_sgd/date=YYYY-MM-DD/
```

---

# 🔄 Airflow DAG Design

## DAG Flow

```
wait_for_transactions (T-1)
        ↓
fetch_exchange_rate (historical T-1)
        ↓
csv_to_parquet (Bronze)
        ↓
spark_transformation (Gold)
        ↓
monitor
```

---

# ⏱️ Realistic Daily Timeline (BEST PRACTICE)

## 📅 Example: Run Date = 2026-04-16

---

## Step 1: Transaction Data

```
05:00 SGT → transactions for 2026-04-15 available
```

---

## Step 2: Exchange Rate Fetch

```
05:05 SGT → API call for 2026-04-15
```

---

## Step 3: Bronze Layer

```
05:10 – 05:25 → CSV → Parquet
```

---

## Step 4: Spark Job

```
05:30 – 05:50 → Transformation
```

---

## ✅ Final Output

```
~06:00 SGT → Gold data ready
```

---

# 📊 SLA (Service Level Agreement)

| Stage                    | SLA       |
| ------------------------ | --------- |
| Transaction availability | 05:00 SGT |
| Pipeline start           | 05:10 SGT |
| Final output             | 06:00 SGT |

---

# 🔁 Idempotency & Backfill

## Idempotency

* Partition-based overwrite
* Safe reruns

---

## Backfill

For each date:

```
transactions(date)
+
exchange_rate(date)
```

---

# ⚠️ Production Edge Cases

---

## ❌ Missing Exchange Rate

**Solution:**

* Retry (3 times)
* Fallback to T-2
* Alert

---

## ❌ Late Transaction Data

**Solution:**

* Sensor wait (max 3 hours)
* SLA alert

---

## ❌ Unknown Currency

**Solution:**

* Send to reject dataset

---

## ❌ Small Files Problem

**Solution:**

* Use coalesce()
* Optimize partitioning

---

# ⚙️ Spark Configuration

```
spark.sql.shuffle.partitions = 200
spark.sql.adaptive.enabled = true
spark.sql.autoBroadcastJoinThreshold = -1
```

---

# ☁️ EMR Cluster (Simulated)

* Instance: m5.xlarge
* Master: 1
* Core: 4–6
* Task: 2–4 (auto-scaling)

---

# 📊 Observability

* Input count
* Valid count
* Reject count
* Output count

---

# 🔐 Compliance

* PII masking implemented
* Secure data handling

---

# 🧠 Interview Summary (Use This!)

> We built a production-grade financial data pipeline in APAC where transaction data arrives on a T-1 basis. To ensure accuracy, we fetch historical exchange rates for the same date and avoid dependency on real-time API availability. The system consists of three pipelines—API ingestion, data standardization, and Spark transformation on EMR. Airflow orchestrates the workflow with proper dependency handling and SLAs. The pipeline ensures data quality through validation, compliance via PII masking, and performance optimization using broadcast joins and partitioned Parquet storage.

---

# 🔥 Key Takeaways

```
T-1 transactions + T-1 exchange rates
No dependency on real-time API
Historical API → deterministic pipeline
Partitioned Parquet → performance
Broadcast join → no shuffle
Airflow sensors → reliability
```

---
