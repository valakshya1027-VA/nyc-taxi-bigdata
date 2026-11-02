"""Run the full batch pipeline: clean -> features -> EDA -> models."""
import argparse
from pathlib import Path

from src import config
from src.clean import clean, read_raw
from src.eda import run_eda
from src.features import add_trip_features, build_demand_table
from src.model import run_models
from src.utils import get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-models", action="store_true", help="only clean + EDA")
    args = ap.parse_args()

    spark = get_spark()
    Path(config.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    raw = read_raw(spark)
    cleaned, report = clean(raw)
    report.to_csv(f"{config.OUTPUT_DIR}/cleaning_report.csv", index=False)
    print(report.to_string(index=False))

    trips = add_trip_features(cleaned)
    trips.write.mode("overwrite").partitionBy("pickup_month").parquet(f"{config.PROCESSED_DIR}/trips_clean")
    trips = spark.read.parquet(f"{config.PROCESSED_DIR}/trips_clean")

    demand = build_demand_table(spark, trips)
    demand.write.mode("overwrite").parquet(f"{config.PROCESSED_DIR}/demand")
    demand = spark.read.parquet(f"{config.PROCESSED_DIR}/demand")

    run_eda(spark, trips, demand)
    if not args.skip_models:
        run_models(demand)
    spark.stop()


if __name__ == "__main__":
    main()
