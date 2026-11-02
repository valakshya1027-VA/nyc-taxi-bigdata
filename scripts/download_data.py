"""Download NYC TLC yellow taxi Parquet files and the zone lookup table."""
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src import config  # noqa: E402


def download(url: str, dest: Path) -> None:
    if dest.exists():
        print(f"skip (exists): {dest.name}")
        return
    print(f"downloading {url}")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)


def main() -> None:
    raw = Path(config.RAW_DIR)
    raw.mkdir(parents=True, exist_ok=True)
    download(config.ZONE_URL, raw / "taxi_zone_lookup.csv")
    for m in config.MONTHS:
        url = config.TRIP_URL.format(year=config.YEAR, month=m)
        download(url, raw / f"yellow_tripdata_{config.YEAR}-{m:02d}.parquet")
    print("done")


if __name__ == "__main__":
    main()
