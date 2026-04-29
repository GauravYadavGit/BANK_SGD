from pyspark.sql.functions import col


def validate_schema(df, expected_columns):
    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise Exception(f"Missing columns: {missing}")
    return df


def validate_data(df):

    # Rule 1: Only one of deposit or withdrawal should exist
    condition = ~(col("withdrawal_amt").isNotNull() & col("deposit_amt").isNotNull())

    valid_df = df.filter(condition)
    reject_df = df.filter(~condition)

    # Rule 2: Amount should not be negative (handle nulls properly)
    valid_df = valid_df.filter(
        (col("deposit_amt").isNull() | (col("deposit_amt") >= 0))
        & (col("withdrawal_amt").isNull() | (col("withdrawal_amt") >= 0))
    )

    return valid_df, reject_df
