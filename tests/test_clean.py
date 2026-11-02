from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from src import config  # noqa: E402
from src.clean import clean  # noqa: E402
from src.utils import get_spark  # noqa: E402


def test_clean_removes_bad_rows():
    spark = get_spark("test")
    y = config.YEAR
    rows = [
        (datetime(y, 3, 1, 10, 0), datetime(y, 3, 1, 10, 20), 10, 20, 1, 3.0, 12.0, 2.0, 15.0, 1, 1),   # good
        (datetime(y, 3, 1, 10, 0), datetime(y, 3, 1, 9, 0), 10, 20, 1, 3.0, 12.0, 2.0, 15.0, 1, 1),     # dropoff first
        (datetime(y, 3, 1, 11, 0), datetime(y, 3, 1, 11, 10), 10, 20, 1, 0.0, 12.0, 2.0, 15.0, 1, 1),   # zero distance
        (datetime(y, 3, 1, 12, 0), datetime(y, 3, 1, 12, 10), 10, 20, 1, 2.0, -5.0, 0.0, -5.0, 1, 1),   # negative fare
    ]
    cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "passenger_count",
            "trip_distance", "fare_amount", "tip_amount", "total_amount", "payment_type", "RatecodeID"]
    out, report = clean(spark.createDataFrame(rows, cols))
    assert out.count() == 1
    assert report.loc[report.rule == "TOTAL_RAW_ROWS", "rows_failing"].iloc[0] == 4
