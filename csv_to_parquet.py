# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit, trim
# from pyspark.sql.types import *

# # -------------------------------
# # 1. Spark Session
# # -------------------------------
# spark = SparkSession.builder.appName("financial_csv_to_parquet").getOrCreate()

# # Performance tuning
# spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
# spark.conf.set("spark.sql.shuffle.partitions", "50")

# # -------------------------------
# # 2. Runtime Parameters
# # -------------------------------
# run_date = "2026-04-17"

# input_path = f"s3://sgdopenexhange/financial-data/transactions/date={run_date}/*.csv"
# output_path = "s3://sgdopenexhange/financial-data/bronze/transactions/"

# print(f"📥 Reading from: {input_path}")
# print(f"📤 Writing to: {output_path}")

# # -------------------------------
# # 3. Schema Definition
# # -------------------------------
# schema = StructType(
#     [
#         StructField("account_no", StringType(), True),
#         StructField("date", StringType(), True),
#         StructField("transaction_details", StringType(), True),
#         StructField("chq_no", StringType(), True),
#         StructField("value_date", StringType(), True),
#         StructField("withdrawal_amt", DoubleType(), True),
#         StructField("withdrawal_currency", StringType(), True),
#         StructField("deposit_amt", DoubleType(), True),
#         StructField("deposit_currency", StringType(), True),
#         StructField("balance_amt", DoubleType(), True),
#     ]
# )

# # -------------------------------
# # 4. Read CSV (Multi-file)
# # -------------------------------
# df = (
#     spark.read.option("header", True)
#     .option("mode", "PERMISSIVE")
#     .option("columnNameOfCorruptRecord", "_corrupt_record")
#     .schema(schema)
#     .csv(input_path)
# )

# print("📊 Data read successfully")

# # -------------------------------
# # 5. Cleanup
# # -------------------------------
# string_cols = [f.name for f in schema.fields if isinstance(f.dataType, StringType)]

# for c in string_cols:
#     df = df.withColumn(c, trim(col(c)))

# # -------------------------------
# # 6. Add Metadata
# # -------------------------------
# df = df.withColumn("ingestion_date", lit(run_date))

# # -------------------------------
# # 7. Repartition (Optimized)
# # -------------------------------
# df = df.repartition(50)

# # -------------------------------
# # 8. Write to Bronze
# # -------------------------------
# (
#     df.write.mode("append")
#     .option("compression", "snappy")
#     .partitionBy("ingestion_date")
#     .parquet(output_path)
# )

# print("✅ Bronze write completed successfully")

# spark.stop()

# ------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("parquet_to_bronze").getOrCreate()

spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
spark.conf.set("spark.sql.shuffle.partitions", "50")

run_date = "2026-04-17"

input_path = (
    f"s3://sgdopenexhange/financial-data/transactions/ingestion_date={run_date}/"
)
output_path = "s3://sgdopenexhange/financial-data/bronze/transactions/test/"

print(f"📥 Reading parquet from: {input_path}")

# Read parquet
df = spark.read.parquet(input_path)

# Cleanup only string columns
string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]

for c in string_cols:
    df = df.withColumn(c, trim(col(c)))

# Add ingestion_date
df = df.withColumn("ingestion_date", lit(run_date))

# Repartition (important for file size control)
df = df.repartition(200)

# Write to bronze
(
    df.write.mode("overwrite")  # ⚠️ careful here
    .partitionBy("ingestion_date")
    .parquet(output_path)
)

print("✅ Done")
spark.stop()
