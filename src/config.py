"""Central configuration. Edit values here or override with environment variables."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Storage locations. For HDFS/S3 set e.g. DATA_DIR=hdfs:///data/nyc_taxi or s3a://bucket/nyc
DATA_DIR = os.getenv("DATA_DIR", str(ROOT / "data"))
RAW_DIR = f"{DATA_DIR}/raw"
PROCESSED_DIR = f"{DATA_DIR}/processed"
OUTPUT_DIR = os.getenv("OUTPUT_DIR", str(ROOT / "outputs"))

# Study period
YEAR = int(os.getenv("YEAR", "2023"))
MONTHS = [int(m) for m in os.getenv("MONTHS", "1,2,3,4,5,6,7,8,9,10,11,12").split(",")]

# Source URLs (verify on the NYC TLC trip record page if a download fails)
TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Cleaning thresholds
MAX_DISTANCE_MILES = 100
MAX_FARE = 500
MAX_PASSENGERS = 6
MAX_TRIP_MINUTES = 360

# Airport zone IDs (EWR, JFK, LaGuardia)
AIRPORT_ZONES = [1, 132, 138]

# Modelling
TEST_DAYS = int(os.getenv("TEST_DAYS", "60"))   # last N days used as test set
SEED = 42

# Spark
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "64")
