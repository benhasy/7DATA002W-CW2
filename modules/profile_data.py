"""
Dataset profiling for Amazon Fine Food Reviews.

Run locally on the sample CSV to validate logic, then run on Databricks
against the full /raw zone to profile the real dataset.

Databricks usage (in a notebook cell):
    # Cell 1: configure storage access
    # Cell 2: paste this file contents (without the main() block)
    # Cell 3: call profile_dataset(spark, "abfss://...", "csv")
"""

import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def profile_dataset(spark: SparkSession, input_path: str, file_format: str = "csv") -> dict:
    """Build a profile dictionary describing the dataset.

    Returns a dict that can be serialised to JSON and saved alongside the report
    as evidence for the rubric's data-quality and rationale criteria.
    """
    reader = spark.read
    if file_format == "csv":
        df = reader.option("header", True).option("inferSchema", True).csv(input_path)
    elif file_format == "json":
        df = reader.json(input_path)
    elif file_format == "parquet":
        df = reader.parquet(input_path)
    else:
        raise ValueError(f"Unsupported file_format: {file_format}")

    profile = {
        "input_path": input_path,
        "format": file_format,
        "profiled_at_utc": datetime.utcnow().isoformat(),
        "row_count": df.count(),
        "column_count": len(df.columns),
        "schema": [{"name": f.name, "type": str(f.dataType)} for f in df.schema.fields],
        "null_counts": {},
        "duplicate_id_count": None,
        "score_distribution": {},
        "time_range": {},
        "candidate_pii_columns": [],
    }

    # Null counts per column in one pass for efficiency
    null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    null_row = df.agg(*null_exprs).collect()[0].asDict()
    profile["null_counts"] = {c: int(null_row[c]) for c in df.columns}

    # Duplicate count on Id column if present
    if "Id" in df.columns:
        total = profile["row_count"]
        distinct_ids = df.select("Id").distinct().count()
        profile["duplicate_id_count"] = total - distinct_ids

    # Score distribution — cast to int to handle string inference
    if "Score" in df.columns:
        score_rows = df.filter(F.col("Score").cast("int").isNotNull()) \
            .groupBy(F.col("Score").cast("int").alias("Score")) \
            .count().orderBy("Score").collect()
        profile["score_distribution"] = {
            int(r["Score"]): int(r["count"]) for r in score_rows
        }

    # Time range — cast to long to handle string inference
    if "Time" in df.columns:
        df_clean = df.filter(F.col("Time").cast("long").isNotNull())
        bounds = df_clean.agg(
            F.min(F.col("Time").cast("long")).alias("min_t"),
            F.max(F.col("Time").cast("long")).alias("max_t")
        ).collect()[0]
        if bounds["min_t"] is not None:
            profile["time_range"] = {
                "min_epoch": int(bounds["min_t"]),
                "max_epoch": int(bounds["max_t"]),
                "min_date": datetime.utcfromtimestamp(int(bounds["min_t"])).isoformat(),
                "max_date": datetime.utcfromtimestamp(int(bounds["max_t"])).isoformat(),
            }

    # Candidate PII heuristic
    pii_keywords = ("user", "name", "email", "address", "phone")
    profile["candidate_pii_columns"] = [
        c for c in df.columns if any(k in c.lower() for k in pii_keywords)
    ]

    return profile