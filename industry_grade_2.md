# 🏦 Financial Data Pipeline — Realistic Production Simulation

---

# 🎯 Objective

Design a production-grade pipeline to:

* Process **multi-currency transactions**
* Convert all values into **SGD**
* Deliver **analytics-ready dataset** daily

---

# 🌍 Domain Context

* Domain: Financial Services / Banking
* Region: APAC (Singapore)
* Reporting Currency: SGD

---

# 📊 Data Profile (FINAL REALISTIC NUMBERS)

## 🟫 Transaction Data

### Daily Volume

```text
~1.5 million transactions/day
```

---

### Record Size

```text
~1.2 KB per record
```

---

### Raw Data Size

```text
~2 GB/day (CSV format)
```

---

## 🟫 Bronze Layer (Parquet)

After compression:

```text
~700 MB/day
```

---

## 🟩 Gold Layer (Final Output)

After transformation:

```text
~500 MB/day
```

---

## 🟦 Exchange Rate Data

```text
~150–200 currencies
~200–500 KB/day
```

---

# 📅 Storage Growth

## Monthly

```text
Raw:    ~60 GB
Bronze: ~20 GB
Gold:   ~15 GB
```

---

## Yearly

```text
Raw:    ~720 GB
Bronze: ~240 GB
Gold:   ~180 GB
```

---

# ⏱️ Data Arrival Pattern

## 🟫 Transaction Data (T-1)

```text
02:00 – 04:00 → Aggregation
04:30 – 05:00 → Available in S3
```

👉 Data received = **previous day (T-1)**

---

## 🟦 Exchange Rate Data

```text
Fetched via historical API
GET /historical/{T-1}.json
```

👉 Always available (no waiting)

---

# ⚖️ Business Logic

```text
Transactions (T-1)
+
Exchange Rate (T-1)
```

---

## 💰 Conversion Formula

```text
SGD = amount / rate_to_sgd
```

---

# 🏗️ Pipeline Architecture

```text
Pipeline 1 → Exchange Rate Ingestion
Pipeline 2 → CSV → Parquet (Bronze)
Pipeline 3 → Spark Transformation (Gold)
```

---

# 🔄 Airflow DAG Flow

```text
wait_for_transactions (T-1)
        ↓
fetch_exchange_rate (T-1)
        ↓
csv_to_parquet (Bronze)
        ↓
spark_transformation (EMR)
        ↓
monitor & validate
```

---

# ⏱️ Daily Execution Timeline (CRITICAL)

## 📅 Example: Run Date = 2026-04-16

---

## Step 1: Transaction Availability

```text
05:00 → T-1 data ready in S3
```

---

## Step 2: DAG Starts

```text
05:00 → Airflow DAG triggered
```

---

## Step 3: Exchange Rate Fetch

```text
05:02 – 05:05 → API call (historical T-1)
```

---

## Step 4: Bronze Conversion

```text
05:05 – 05:15 → CSV → Parquet
Duration: ~10 mins
```

---

## Step 5: Spark Transformation (EMR)

```text
05:15 – 05:45 → Spark job execution
Duration: ~25–30 mins
```

---

### Inside Spark Job

* Read Bronze data (~700 MB)
* Broadcast exchange rates
* Perform join + transformation
* Apply validation + masking

---

## Step 6: Final Write (Gold Layer)

```text
05:45 – 05:55 → Write output to S3
```

---

## ✅ Final SLA

```text
~06:00 SGT → Data available for BI/reporting
```

---

# ⚙️ EMR Cluster Configuration

```text
Master: 1 × m5.xlarge
Core:   3 × m5.xlarge
Task:   2 × spot (optional)
```

---

# ⚙️ Spark Configuration

```text
Executors: ~10–12
Executor Cores: 2
Executor Memory: 3–4 GB

spark.sql.shuffle.partitions = 50
spark.sql.adaptive.enabled = true
```

---

# ⚡ Performance Characteristics

```text
Input: ~2 GB
Processed: ~700 MB
Output: ~500 MB
```

---

## Runtime

```text
Total pipeline time: ~45–60 minutes
Spark job time: ~25–30 minutes
```

---

# 🔁 Idempotency & Backfill

* Partition-based processing (`date=YYYY-MM-DD`)
* Safe overwrite on rerun
* Airflow catchup enabled

---

# 🚨 Production Edge Cases

## Missing Transaction Data

* Sensor wait
* SLA alert

---

## API Failure

* Retry (3 times)
* Fallback to previous day

---

## Data Quality Issues

* Invalid records → reject dataset

---

# 📊 Observability

* Input count
* Valid vs reject count
* Processing time
* Spark metrics

---

# 🔐 Compliance

* PII masking (account number hashed)
* Secure S3 storage

---

# 🧠 Interview Summary (USE THIS)

> We designed a financial data pipeline processing around 1.5 million transactions per day (~2 GB raw data). The pipeline processes T-1 data and enriches it with historical exchange rates to ensure financial accuracy. It consists of three layers—API ingestion, Bronze standardization, and Spark transformation on EMR. The system is orchestrated using Airflow and completes within one hour, delivering analytics-ready data by 6 AM for reporting.

---

# 🔥 Key Takeaways

```text
~1.5M records/day
~2 GB raw data/day
T-1 processing model
Historical API usage
~1 hour SLA
EMR + Spark optimized
```

---
