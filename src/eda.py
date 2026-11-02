"""Stage 3: exploratory analysis. Each result is aggregated in Spark then saved as a small CSV."""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from src import config


def _save(df: DataFrame, name: str) -> None:
    out = Path(config.OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    df.toPandas().to_csv(out / f"{name}.csv", index=False)
    print(f"saved outputs/{name}.csv")


def run_eda(spark: SparkSession, trips: DataFrame, demand: DataFrame) -> None:
    zones = spark.read.csv(f"{config.RAW_DIR}/taxi_zone_lookup.csv", header=True, inferSchema=True)

    # 1. Peak hours: average city-wide pickups per hour by weekday
    city = demand.groupBy("hour_ts", "dow", "hour").agg(F.sum("pickups").alias("pickups"))
    _save(city.groupBy("dow", "hour").agg(F.avg("pickups").alias("avg_pickups")).orderBy("dow", "hour"), "hourly_pattern")

    # 2. Busiest zones
    top = (
        trips.groupBy("PULocationID").agg(F.count("*").alias("pickups"), F.avg("fare_amount").alias("avg_fare"))
        .join(zones, F.col("PULocationID") == F.col("LocationID"), "left")
        .select("PULocationID", "Borough", "Zone", "pickups", "avg_fare")
        .orderBy(F.desc("pickups"))
    )
    _save(top, "zone_pickups")

    # 3. Fare vs distance (1-mile bins)
    fare = (
        trips.withColumn("dist_bin", F.floor("trip_distance"))
        .filter(F.col("dist_bin") <= 30)
        .groupBy("dist_bin")
        .agg(F.avg("fare_amount").alias("avg_fare"), F.avg("duration_min").alias("avg_duration_min"), F.count("*").alias("trips"))
        .orderBy("dist_bin")
    )
    _save(fare, "fare_vs_distance")

    # 4. Tipping behaviour (card payments only: cash tips are not recorded)
    tips = (
        trips.filter(F.col("payment_type") == 1)
        .groupBy("pickup_hour")
        .agg(F.avg("tip_pct").alias("avg_tip_pct"), F.count("*").alias("trips"))
        .orderBy("pickup_hour")
    )
    _save(tips, "tips_by_hour")
    _save(
        trips.groupBy("payment_type").agg(F.avg("tip_pct").alias("avg_tip_pct"), F.count("*").alias("trips")).orderBy("payment_type"),
        "tips_by_payment_type",
    )

    # 5. Airport vs non-airport trips
    airport = trips.groupBy("is_airport").agg(
        F.count("*").alias("trips"), F.avg("trip_distance").alias("avg_distance"),
        F.avg("fare_amount").alias("avg_fare"), F.avg("duration_min").alias("avg_duration_min"),
        F.avg("tip_pct").alias("avg_tip_pct"),
    )
    _save(airport, "airport_vs_other")

    # 6. Monthly trend
    _save(trips.groupBy("pickup_month").agg(F.count("*").alias("trips"), F.avg("fare_amount").alias("avg_fare")).orderBy("pickup_month"), "monthly_trend")
