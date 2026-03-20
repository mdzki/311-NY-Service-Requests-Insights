"""Extract and load tasks for NYC 311 data."""

import polars as pl
from pathlib import Path
import requests
from prefect import task, get_run_logger
from prefect.variables import Variable
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3, log_prints=True)
def download_data(year: int, month: int) -> Path:
    """Download NYC 311 data from API for a specific month."""
    logger = get_run_logger()
    base_url = Variable.get(
        "nyc_311_url",
        default="https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
    )
    limit = Variable.get("nyc_311_limit", default="1000000")

    file_name = f"nyc_311_{year}_{month:02d}.csv"
    path = Path(f"data/{file_name}")
    file_name_parquet = f"nyc_311_{year}_{month:02d}.parquet"
    path_parquet = Path(f"data/{file_name_parquet}")

    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        logger.info(f"File {file_name} already exists, skipping download.")
        return path
    elif path_parquet.exists():
        logger.info(f"Parquet file {file_name_parquet} already exists, skipping download.")
        return path

    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    params = {
        "$where": (
            f"created_date >= '{year}-{month:02d}-01T00:00:00' AND "
            f"created_date < '{next_year}-{next_month:02d}-01T00:00:00'"
        ),
        "$limit": limit
    }

    logger.info(f"Downloading {file_name}...")
    try:
        with requests.get(base_url, params=params, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except requests.RequestException as e:
        logger.error(f"Download failed: {e}")
        raise
    return path


@task(log_prints=True)
def format_to_parquet(csv_path: Path) -> Path:
    """Convert CSV to Parquet format."""
    logger = get_run_logger()
    parquet_path = csv_path.with_suffix(".parquet")
    if parquet_path.exists():
        logger.info(f"Parquet file {parquet_path.name} already exists, skipping conversion.")
        return parquet_path
    
    logger.info(f"Converting {csv_path.name} to Parquet...")
    df = pl.read_csv(csv_path, infer_schema_length=50000, ignore_errors=True)
    df.write_parquet(parquet_path)
    return parquet_path


@task(log_prints=True)
def upload_to_gcs(path: Path) -> None:
    """Upload file to GCS bucket."""
    logger = get_run_logger()
    gcs_block = GcsBucket.load("nyc-311-bucket")
    logger.info(f"Uploading {path.name} to GCS...")
    gcs_block.upload_from_path(from_path=path, to_path=f"311/{path.name}")


@task(log_prints=True)
def validate_parquet_and_cleanup(parquet_path: Path) -> None:
    """Validate row count between Parquet and corresponding CSV, and delete CSV if they match."""
    logger = get_run_logger()
    csv_path = parquet_path.with_suffix(".csv")

    if not parquet_path.exists():
        logger.warning(f"Parquet not found: {parquet_path}")
        return

    if not csv_path.exists():
        logger.warning(f"CSV not found: {csv_path}")
        return

    try:
        parquet_count = pl.scan_parquet(parquet_path).select(pl.count()).collect().item()
        csv_count = pl.scan_csv(csv_path, ignore_errors=True).select(pl.count()).collect().item()
    except Exception as exc:
        logger.error(f"Failed to count rows: {exc}")
        raise

    if csv_count == parquet_count:
        logger.info(
            f"Row counts match (csv={csv_count}, parquet={parquet_count}); deleting {csv_path.name}."
        )
        try:
            csv_path.unlink()
        except Exception as exc:
            logger.error(f"Failed to delete CSV {csv_path}: {exc}")
            raise
    else:
        logger.warning(
            f"Row count mismatch: csv={csv_count}, parquet={parquet_count}; keeping CSV for inspection."
        )


@task(log_prints=True)
def upload_to_gcs(path: Path) -> None:
    """Upload file to GCS bucket."""
    gcs_block = GcsBucket.load("nyc-311-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=f"311/{path.name}")