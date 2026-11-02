"""Stage 1: ingest raw Parquet and clean it. Records how many rows each rule removes."""
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F

from src import config

REQUIRED = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
    "passenger_count", "trip_distance", "fare_amount", "tip_amount",
    "total_amount", "payment_type", "RatecodeID",
]


def read_raw(spark: SparkSession) -> DataFrame:
    paths = [f"{config.RAW_DIR}/yellow_tripdata_{config.YEAR}-{m:02d}.parquet" for m in config.MONTHS]
    df = spark.read.option("mergeSchema", "true").parquet(*paths)
    # Parquet schemas drift between months (int vs double), so cast to stable types.
    return (
        df.select(*REQUIRED)
        .withColumn("PULocationID", F.col("PULocationID").cast("int"))
        .withColumn("DOLocationID", F.col("DOLocationID").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("int"))
        .withColumn("payment_type", F.col("payment_type").cast("int"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("tip_amount", F.col("tip_amount").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
    )


def clean(df: DataFrame):
    """Return (clean_df, report_pdf). Each rule's impact is counted independently."""
    start = F.to_timestamp(F.lit(f"{config.YEAR}-01-01"))
    end = F.to_timestamp(F.lit(f"{config.YEAR + 1}-01-01"))
    pu, do = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    minutes = (F.unix_timestamp(do) - F.unix_timestamp(pu)) / 60.0

    rules = [
        ("null_required_fields", F.col("PULocationID").isNotNull() & pu.isNotNull() & do.isNotNull()),
        ("pickup_outside_study_year", (pu >= start) & (pu < end)),
        ("dropoff_not_after_pickup", do > pu),
        ("duration_over_limit", minutes <= config.MAX_TRIP_MINUTES),
        ("distance_out_of_range", (F.col("trip_distance") > 0) & (F.col("trip_distance") < config.MAX_DISTANCE_MILES)),
        ("fare_out_of_range", (F.col("fare_amount") > 0) & (F.col("fare_amount") < config.MAX_FARE)),
        ("passenger_out_of_range", (F.col("passenger_count") > 0) & (F.col("passenger_count") <= config.MAX_PASSENGERS)),
        ("negative_tip", F.col("tip_amount") >= 0),
    ]

    # One pass to count how many rows fail each rule independently.
    fail_exprs = [F.sum(F.when(~F.coalesce(cond, F.lit(False)), 1).otherwise(0)).alias(name) for name, cond in rules]
    stats = df.agg(F.count("*").alias("total_rows"), *fail_exprs).collect()[0].asDict()

    out = df
    for _, cond in rules:
        out = out.filter(F.coalesce(cond, F.lit(False)))
    out = out.dropDuplicates()

    total = stats.pop("total_rows")
    report = pd.DataFrame(
        [{"rule": k, "rows_failing": int(v or 0), "pct_of_raw": round(100.0 * (v or 0) / total, 3)} for k, v in stats.items()]
    )
    report.loc[len(report)] = ["TOTAL_RAW_ROWS", total, 100.0]
    return out, report
