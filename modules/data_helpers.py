# data_helpers.py
# Reusable helper functions for the CW2 data pipeline
# Used by both notebook 01 (raw to processed) and notebook 02 (processed to curated)

from pyspark.sql.functions import col, sha2, concat, lit, when, from_unixtime, year, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def get_expected_schema():
    """Returns the expected schema for the raw Amazon reviews CSV."""
    return StructType([
        StructField("Id", IntegerType(), True),
        StructField("ProductId", StringType(), True),
        StructField("UserId", StringType(), True),
        StructField("ProfileName", StringType(), True),
        StructField("HelpfulnessNumerator", IntegerType(), True),
        StructField("HelpfulnessDenominator", IntegerType(), True),
        StructField("Score", IntegerType(), True),
        StructField("Time", IntegerType(), True),
        StructField("Summary", StringType(), True),
        StructField("Text", StringType(), True)
    ])


def check_null_counts(df):
    """Logs null counts for every column in the dataframe."""
    null_counts = df.select([
        col(c).isNull().cast("int").alias(c)
        for c in df.columns
    ]).groupBy().sum().collect()[0].asDict()

    print("Null counts per column:")
    for col_name, count in null_counts.items():
        print(f"  {col_name}: {count}")
    return null_counts


def quarantine_invalid_rows(df, quarantine_path):
    """
    Splits dataframe into valid and invalid rows.
    Invalid rows (null Score or Id) are written to the quarantine path.
    Returns the valid dataframe.
    """
    df_quarantine = df.filter(col("Score").isNull() | col("Id").isNull())
    quarantine_count = df_quarantine.count()

    if quarantine_count > 0:
        df_quarantine.write.mode("overwrite").parquet(quarantine_path)
        print(f"Quarantined {quarantine_count} rows to: {quarantine_path}")
    else:
        print("No rows quarantined.")

    df_valid = df.filter(col("Score").isNotNull() & col("Id").isNotNull())
    return df_valid, quarantine_count


def anonymise(df, salt):
    """
    Anonymises PII fields:
    - UserId: SHA-256 hashed with salt from Key Vault
    - ProfileName: fully redacted
    """
    return df \
        .withColumn("UserId", sha2(concat(col("UserId"), lit(salt)), 256)) \
        .withColumn("ProfileName", lit("REDACTED"))


def add_derived_columns(df):
    """
    Adds derived columns for downstream analysis:
    - ReviewDate, ReviewYear, ReviewMonth from unix timestamp
    - HelpfulnessRatio (null where denominator is 0)
    - SentimentBand (Positive/Neutral/Negative)
    """
    return df \
        .withColumn("ReviewDate", from_unixtime(col("Time")).cast("date")) \
        .withColumn("ReviewYear", year(from_unixtime(col("Time")))) \
        .withColumn("ReviewMonth", month(from_unixtime(col("Time")))) \
        .withColumn("HelpfulnessRatio",
            when(col("HelpfulnessDenominator") == 0, None)
            .otherwise(col("HelpfulnessNumerator") / col("HelpfulnessDenominator"))
        ) \
        .withColumn("SentimentBand",
            when(col("Score") >= 4, "Positive")
            .when(col("Score") == 3, "Neutral")
            .otherwise("Negative")
        ) \
        .drop("Time")


def fail_fast_check(raw_count, valid_count, threshold=0.5):
    """
    Raises an exception if too many rows were lost during cleaning.
    Default threshold is 50% row loss.
    """
    if valid_count < raw_count * threshold:
        raise Exception(
            f"FAIL: Too many rows lost. Raw: {raw_count}, Valid: {valid_count}. "
            f"Pipeline halted to prevent corrupt output."
        )
    print(f"Row count check passed. Raw: {raw_count}, Valid: {valid_count}")