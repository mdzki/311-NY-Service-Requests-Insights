"""Extract and load tasks for NYC 311 data."""

import polars as pl
from pathlib import Path
import requests
from prefect import task
from prefect.variables import Variable
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3, log_prints=True)
def download_data(year: int, month: int) -> Path:
    """Download NYC 311 data from API for a specific month."""
    base_url = Variable.get(
        "nyc_311_url",
        default="https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
    )
    limit = Variable.get("nyc_311_limit", default="1000000")

    file_name = f"nyc_311_{year}_{month:02d}.csv"
    path = Path(f"data/{file_name}")
    path.parent.mkdir(parents=True, exist_ok=True)

    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    params = {
        "$where": (
            f"created_date >= '{year}-{month:02d}-01T00:00:00' AND "
            f"created_date < '{next_year}-{next_month:02d}-01T00:00:00'"
        ),
        "$limit": limit
    }

    print(f"Downloading {file_name}...")
    with requests.get(base_url, params=params, stream=True) as r:
        r.raise_for_status()
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return path


@task(log_prints=True)
def format_to_parquet(csv_path: Path) -> Path:
    """Convert CSV to Parquet format."""
    parquet_path = csv_path.with_suffix(".parquet")
    df = pl.read_csv(csv_path, infer_schema_length=50000, ignore_errors=True)
    df.write_parquet(parquet_path)
    return parquet_path


@task(log_prints=True)
def upload_to_gcs(path: Path) -> None:
    """Upload file to GCS bucket."""
    gcs_block = GcsBucket.load("nyc-311-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=f"311/{path.name}")