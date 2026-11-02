"""Spark Structured Streaming job: 15-minute sliding-window pickups per zone from Kafka.

Run with the Kafka connector matching your Spark/Scala version, for example:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/streaming/consumer.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType  # noqa: E402

from src.utils import get_spark  # noqa: E402

schema = StructType([
    StructField("pickup_ts", StringType()),
    StructField("zone", IntegerType()),
    StructField("fare", DoubleType()),
])


def main(bootstrap="localhost:9092", topic="taxi-trips"):
    spark = get_spark("TaxiStreaming")
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest").load())

    events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")
              .withColumn("ts", F.to_timestamp("pickup_ts")))

    agg = (events.withWatermark("ts", "30 minutes")
           .groupBy(F.window("ts", "15 minutes", "5 minutes"), "zone")
           .agg(F.count("*").alias("pickups"), F.avg("fare").alias("avg_fare")))

    query = (agg.writeStream.outputMode("update").format("console")
             .option("truncate", "false").option("checkpointLocation", "checkpoints/taxi").start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
