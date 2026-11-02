"""Compare Pandas (single machine) vs Spark for the same aggregation as the dataset grows."""
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from src import config
from src.utils import get_spark


def pandas_job(files):
    t0 = time.time()
    cols = ["tpep_pickup_datetime", "PULocationID", "trip_distance", "fare_amount"]
    df = pd.concat([pd.read_parquet(f, columns=cols) for f in files])
    df = df[(df.trip_distance > 0) & (df.fare_amount > 0)]
    df["hour_ts"] = df.tpep_pickup_datetime.dt.floor("h")
    df.groupby(["PULocationID", "hour_ts"]).size()
    return time.time() - t0


def spark_job(spark, files):
    t0 = time.time()
    df = spark.read.parquet(*files).select("tpep_pickup_datetime", "PULocationID", "trip_distance", "fare_amount")
    df = df.filter((F.col("trip_distance") > 0) & (F.col("fare_amount") > 0))
    df.groupBy("PULocationID", F.date_trunc("hour", "tpep_pickup_datetime")).count().collect()
    return time.time() - t0


def main():
    spark = get_spark("Benchmark")
    all_files = [f"{config.RAW_DIR}/yellow_tripdata_{config.YEAR}-{m:02d}.parquet" for m in config.MONTHS]
    rows = []
    for n in (1, 3, 6, 12):
        files = all_files[:n]
        if len(files) < n:
            break
        try:
            p = pandas_job(files)
        except MemoryError:
            p = float("nan")
        s = spark_job(spark, files)
        rows.append({"months": n, "pandas_seconds": round(p, 2), "spark_seconds": round(s, 2)})
        print(rows[-1])
    Path(config.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(f"{config.OUTPUT_DIR}/benchmark.csv", index=False)


if __name__ == "__main__":
    main()
