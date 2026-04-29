# import sys
# import json
# import pkgutil
# from datetime import datetime
# import logging

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, coalesce, broadcast, lit


# # -------------------------------
# # Logging Setup
# # -------------------------------
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# # -------------------------------
# # Load Config
# # -------------------------------
# def load_config():
#     data = pkgutil.get_data("BANK_SGD.config", "config.json")
#     return json.loads(data.decode("utf-8"))


# # -------------------------------
# # Read Transactions (Partition-based)
# # -------------------------------
# def read_transactions(spark, base_path, run_date):
#     path = f"{base_path}/ingestion_date={run_date}"
#     logger.info(f"Reading transactions from: {path}")
#     return spark.read.parquet(path)


# # -------------------------------
# # Read FX Data (Partition-based)
# # -------------------------------
# def read_fx_data(spark, base_path, run_date):
#     dt = datetime.strptime(run_date, "%Y-%m-%d")
#     path = f"{base_path}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
#     logger.info(f"Reading FX data from: {path}")
#     return spark.read.parquet(path)


# # -------------------------------
# # Prepare FX
# # -------------------------------
# def prepare_fx(df):
#     return df.withColumnRenamed("target_currency", "currency")


# # -------------------------------
# # Validate Transactions
# # -------------------------------
# def validate_transactions(df):
#     return df.filter(
#         (col("withdrawal_amt").isNotNull() | col("deposit_amt").isNotNull())
#         & (col("withdrawal_currency").isNotNull() | col("deposit_currency").isNotNull())
#     )


# # -------------------------------
# # Add Currency Column
# # -------------------------------
# def add_currency_column(df):
#     return df.withColumn(
#         "currency", coalesce(col("withdrawal_currency"), col("deposit_currency"))
#     )


# # -------------------------------
# # Enrich with FX (Broadcast Join)
# # -------------------------------
# def enrich_with_fx(txn_df, fx_df):
#     df = txn_df.join(broadcast(fx_df), on="currency", how="left")
#     return df.filter(col("rate_to_sgd").isNotNull())


# # -------------------------------
# # Write Output (with repartition)
# # -------------------------------
# def write_output(df, path, partition_col):
#     logger.info(f"Writing output to: {path}")

#     # Repartition for controlled file size
#     df = df.repartition(8)

#     df.write.mode("overwrite").partitionBy(partition_col).parquet(path)


# # -------------------------------
# # MAIN
# # -------------------------------
# def main(run_date):

#     spark = SparkSession.builder.appName("Transaction Pipeline").getOrCreate()

#     logger.info(f"🚀 Running pipeline for {run_date}")

#     config = load_config()

#     bronze_path = config["bronze_path"]
#     fx_path = config["fx_path"]
#     silver_path = config["silver_path"]

#     # -------------------------------
#     # 1. Read
#     # -------------------------------
#     txn_df = read_transactions(spark, bronze_path, run_date)
#     fx_df = read_fx_data(spark, fx_path, run_date)

#     # -------------------------------
#     # 2. Prepare FX
#     # -------------------------------
#     fx_df = prepare_fx(fx_df)

#     # -------------------------------
#     # 3. Validate
#     # -------------------------------
#     valid_df = validate_transactions(txn_df)

#     # -------------------------------
#     # 4. Add Currency
#     # -------------------------------
#     valid_df = add_currency_column(valid_df)

#     # -------------------------------
#     # 5. Join (Broadcast)
#     # -------------------------------
#     success_df = enrich_with_fx(valid_df, fx_df)

#     success_df = success_df.withColumn("ingestion_date", lit(run_date))

#     # -------------------------------
#     # 6. Write (Action)
#     # -------------------------------
#     write_output(success_df, silver_path, "ingestion_date")

#     logger.info("✅ Pipeline completed successfully")

#     spark.stop()


# # -------------------------------
# # ENTRY
# # -------------------------------
# if __name__ == "__main__":
#     run_date = sys.argv[1]
#     main(run_date)


# --------

# aggregate all currencywise transactions for a given date and write to silver


import sys
import json
import pkgutil
from datetime import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, broadcast, lit, sum as _sum, count


# -------------------------------
# Logging Setup
# -------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -------------------------------
# Load Config
# -------------------------------
def load_config():
    data = pkgutil.get_data("BANK_SGD.config", "config.json")
    return json.loads(data.decode("utf-8"))


# -------------------------------
# Read Transactions
# -------------------------------
def read_transactions(spark, base_path, run_date):
    path = f"{base_path}/ingestion_date={run_date}"
    logger.info(f"Reading transactions from: {path}")
    return spark.read.parquet(path)


# -------------------------------
# Read FX Data
# -------------------------------
def read_fx_data(spark, base_path, run_date):
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    path = f"{base_path}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    logger.info(f"Reading FX data from: {path}")
    return spark.read.parquet(path)


# -------------------------------
# Prepare FX
# -------------------------------
def prepare_fx(df):
    return df.withColumnRenamed("target_currency", "currency")


# -------------------------------
# Validate Transactions
# -------------------------------
def validate_transactions(df):
    return df.filter(
        (col("withdrawal_amt").isNotNull() | col("deposit_amt").isNotNull())
        & (col("withdrawal_currency").isNotNull() | col("deposit_currency").isNotNull())
    )


# -------------------------------
# Add Currency Column
# -------------------------------
def add_currency_column(df):
    return df.withColumn(
        "currency", coalesce(col("withdrawal_currency"), col("deposit_currency"))
    )


# -------------------------------
# Add Amount Column
# -------------------------------
def add_amount_column(df):
    return df.withColumn("amount", coalesce(col("withdrawal_amt"), col("deposit_amt")))


# -------------------------------
# Enrich with FX
# -------------------------------
def enrich_with_fx(txn_df, fx_df):
    df = txn_df.join(broadcast(fx_df), on="currency", how="left")
    return df.filter(col("rate_to_sgd").isNotNull())


# -------------------------------
# Convert to SGD
# -------------------------------
def convert_to_sgd(df):
    return df.withColumn("amount_sgd", col("amount") * col("rate_to_sgd"))


# -------------------------------
# Aggregate by Currency (SKU)
# -------------------------------
def aggregate_by_currency(df, run_date):
    return (
        df.groupBy("currency")
        .agg(count("*").alias("txn_count"), _sum("amount_sgd").alias("total_value_sgd"))
        .withColumn("ingestion_date", lit(run_date))
    )


# -------------------------------
# Write Output
# -------------------------------
def write_output(df, path, partition_col):
    logger.info(f"Writing output to: {path}")

    df = df.coalesce(1)  # Optional: Reduce to 1 file for easier downstream processing

    df.write.mode("overwrite").partitionBy(partition_col).parquet(path)


# -------------------------------
# MAIN
# -------------------------------
def main(run_date):

    spark = SparkSession.builder.appName("Transaction SKU Pipeline").getOrCreate()

    logger.info(f"🚀 Running pipeline for {run_date}")

    config = load_config()

    bronze_path = config["bronze_path"]
    fx_path = config["fx_path"]
    silver_path = config["silver_path"]

    # 1. Read
    txn_df = read_transactions(spark, bronze_path, run_date)
    fx_df = prepare_fx(read_fx_data(spark, fx_path, run_date))

    # 2. Validate
    valid_df = validate_transactions(txn_df)

    # 3. Add business columns
    valid_df = add_currency_column(valid_df)
    valid_df = add_amount_column(valid_df)

    # 4. Join FX
    enriched_df = enrich_with_fx(valid_df, fx_df)

    # 5. Convert to SGD
    enriched_df = convert_to_sgd(enriched_df)

    # 🔥 6. Aggregation (Key Step)
    agg_df = aggregate_by_currency(enriched_df, run_date)

    # 7. Write
    write_output(agg_df, silver_path + "/currency_agg", "ingestion_date")

    logger.info("✅ Pipeline completed successfully")

    spark.stop()


# -------------------------------
# ENTRY
# -------------------------------
if __name__ == "__main__":
    run_date = sys.argv[1]
    main(run_date)
