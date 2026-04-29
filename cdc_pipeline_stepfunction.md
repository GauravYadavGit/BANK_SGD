# 🚀 Enterprise CDC Data Pipeline – Version 3 (Final)

---

# 🔷 1. Overview

This pipeline captures Change Data Capture (CDC) from a source database and processes it into a structured, analytics-ready Data Warehouse.

---

# 🔷 2. End-to-End Flow

```
Source DB
→ DMS (CDC)
→ S3 Raw (CSV)
→ Glue (Schema Evolution + Parquet + Compaction)
→ S3 Curated (Parquet)
→ Spectrum (External Tables)
→ ODS (Latest State)
→ Data Warehouse (Fact + Dimension)
→ BI / Reporting
```

---

# 🔷 3. Raw Layer (S3 - CSV)

* Data arrives via CDC
* Multiple small files
* Schema evolves over time

## Challenges:

* Inconsistent schema
* High file count
* Not query efficient

---

# 🔷 4. Transformation Layer (Glue)

## Responsibilities:

### 1. Schema Evolution Handling

* Define target schema
* Add missing columns as NULL
* Enforce data types

### 2. File Optimization

* Convert CSV → Parquet
* Compact small files

### 3. Partitioning

* Partition by date (and optionally hour)

---

# 🔷 5. Curated Layer (S3 - Parquet)

* Consistent schema across all partitions
* Optimized for querying
* Backward compatibility ensured

---

# 🔷 6. Metadata Layer

* Glue Data Catalog stores schema
* Schema updated when new columns arrive

---

# 🔷 7. Redshift Spectrum

## Features:

* Query data directly from S3
* Uses partition projection

## Optimization:

* Partition pruning
* Column pruning
* Parquet format

---

# 🔷 8. Partition Strategy

## Recommended:

* date=YYYY-MM-DD
* Optional: hour-level partitioning

## Best Approach:

* Partition Projection (no manual partition management)

---

# 🔷 9. ODS Layer (Operational Data Store)

## Purpose:

* Store latest state of data
* Act as source of truth

## Logic:

### 1. Deduplication

```
ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC)
```

### 2. Merge Logic

```
MERGE INTO ods
USING deduplicated data
```

### 3. CDC Handling

* INSERT
* UPDATE
* DELETE

---

# 🔷 10. Staging (Logical)

* Not always a physical table
* Implemented as:

  * Subquery
  * CTE
  * Temp table

## Purpose:

* Deduplication
* Snapshot control
* Performance optimization

---

# 🔷 11. Orchestration

## Step Function Flow:

1. Trigger Glue Job
2. Convert to Parquet
3. Load via Spectrum
4. Merge into ODS
5. Load DW
6. Run reconciliation

---

# 🔷 12. Schema Evolution Handling

## Approach:

* Controlled in Glue
* Not via crawler

## Strategy:

* Add missing columns
* Cast data types
* Maintain backward compatibility

---

# 🔷 13. Data Warehouse Layer

## Components:

### Dimension Tables

* Customer
* Policy

### Fact Tables

* Claims

---

# 🔷 14. SCD Type 2 (Dimension Handling)

## Concept:

* Track history of changes

## Logic:

### Expire old record

```
UPDATE dim
SET end_date = current_date
WHERE end_date IS NULL
```

### Insert new record

```
INSERT new row with start_date
```

---

# 🔷 15. Fact Table Load

## Strategy:

* Incremental load
* Join with dimension to get surrogate keys

---

# 🔷 16. Schema Evolution Example

* New column appears (email, phone)
* Old partitions remain unchanged physically
* Queries return NULL for missing columns

---

# 🔷 17. Key Concepts

## ODS

* Latest state
* No history

## Dimension

* Historical tracking

## Fact

* Transactional data

## Surrogate Key

* Internal generated key

---

# 🔷 18. Trigger Strategy

* Daily batch (midnight)
* Process previous day data

---

# 🔷 19. Reconciliation

* Record count validation
* Aggregation checks

---

# 🔷 20. Debugging Strategy

* Check Step Function logs
* Check Spectrum queries
* Validate S3 data

---

# 🔷 21. Final Strength

* Scalable
* Idempotent
* Cost optimized
* Production ready

---

# 🔷 22. Interview Summary

This pipeline uses CDC ingestion into S3, processes schema evolution via Glue, leverages Spectrum for querying, maintains latest state in ODS, and builds a dimensional model using SCD Type 2 for analytics.

---

# ✅ End of Version 3
