from pyspark.sql.functions import col, trim
from pyspark.sql.types import FloatType


def transform_transactions(df, df_exchange):

    df_exchange_dep = df_exchange.alias("ex_dep")
    df_exchange_wd = df_exchange.alias("ex_wd")

    # ---- Deposit Join ----
    df_dep = df.join(
        df_exchange_dep,
        (col("value_date") == col("ex_dep.run_date"))
        & (trim(col("deposit_currency")) == trim(col("ex_dep.target_currency"))),
        "left",
    ).withColumn(
        "deposit_sgd_amt",
        col("deposit_amt").cast(FloatType())
        / col("ex_dep.rate_to_sgd").cast(FloatType()),
    )

    # ---- Withdrawal Join ----
    df_final = df_dep.join(
        df_exchange_wd,
        (col("value_date") == col("ex_wd.run_date"))
        & (trim(col("withdrawal_currency")) == trim(col("ex_wd.target_currency"))),
        "left",
    ).withColumn(
        "withdrawal_sgd_amt",
        col("withdrawal_amt").cast(FloatType())
        / col("ex_wd.rate_to_sgd").cast(FloatType()),
    )

    return df_final
