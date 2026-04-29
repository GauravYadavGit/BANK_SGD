from pyspark.sql.functions import sha2, col


def mask_pii(df):
    return df.withColumn("account_no_hash", sha2(col("account_no"), 256)).drop(
        "account_no"
    )
