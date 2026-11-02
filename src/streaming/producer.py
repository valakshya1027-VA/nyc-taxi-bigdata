"""Replay historical trips into a Kafka topic to simulate a live feed.
Usage: python -m src.streaming.producer --rate 200
"""
import argparse
import json
import time

import pandas as pd
from kafka import KafkaProducer

from src import config


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="taxi-trips")
    ap.add_argument("--rate", type=int, default=200, help="events per second")
    ap.add_argument("--month", type=int, default=config.MONTHS[0])
    args = ap.parse_args()

    path = f"{config.RAW_DIR}/yellow_tripdata_{config.YEAR}-{args.month:02d}.parquet"
    df = pd.read_parquet(path, columns=["tpep_pickup_datetime", "PULocationID", "fare_amount"]).dropna()
    df = df.sort_values("tpep_pickup_datetime").head(100_000)

    producer = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=lambda v: json.dumps(v).encode())
    for rec in df.itertuples(index=False):
        producer.send(args.topic, {
            "pickup_ts": rec.tpep_pickup_datetime.isoformat(),
            "zone": int(rec.PULocationID),
            "fare": float(rec.fare_amount),
        })
        time.sleep(1.0 / args.rate)
    producer.flush()


if __name__ == "__main__":
    main()
