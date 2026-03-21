"""Extract and load tasks for NYC 311 data."""

from dotenv import load_dotenv
import os
import polars as pl
from pathlib import Path
import requests
from google.cloud import storage
from prefect import task, get_run_logger
from prefect.variables import Variable

load_dotenv(override=True)

project_id = os.getenv("GCP_PROJECT_ID")
creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
bucket_name = os.getenv("GCP_GCS_BUCKET")


@task(retries=3, log_prints=True)
def check_if_exists_in_gcs(year: int, month: int) -> bool:
    """Check if the final parquet file already exists in GCS."""
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob_name = f"ny_service_requests/nyc_311_{year}_{month:02d}.parquet"
    return bucket.blob(blob_name).exists()


@task(retries=3, log_prints=True)
def download_data(year: int, month: int) -> Path:
    """Download NYC 311 data from API for a specific month."""
    logger = get_run_logger()
    base_url = Variable.get(
        "nyc_311_url", default="https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
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
        logger.info(
            f"Parquet file {file_name_parquet} already exists, skipping download."
        )
        return path

    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    params = {
        "$where": (
            f"created_date >= '{year}-{month:02d}-01T00:00:00' AND "
            f"created_date < '{next_year}-{next_month:02d}-01T00:00:00'"
        ),
        "$limit": limit,
    }

    logger.info(f"Downloading {file_name}...")
    try:
        with requests.get(base_url, params=params, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
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
        logger.info(
            f"Parquet file {parquet_path.name} already exists, skipping conversion."
        )
        return parquet_path

    logger.info(f"Converting {csv_path.name} to Parquet...")
    df = pl.read_csv(csv_path, infer_schema_length=50000, ignore_errors=True)
    df.write_parquet(parquet_path)
    return parquet_path


@task(log_prints=True)
def validate_parquet_with_csv_and_cleanup(parquet_path: Path) -> Path:
    """Validate row count between Parquet and corresponding CSV, and delete CSV if they match."""
    logger = get_run_logger()
    csv_path = parquet_path.with_suffix(".csv")

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet not found: {parquet_path}")

    if not csv_path.exists():
        logger.warning(f"CSV not found: {csv_path}. Nothing to compare or delete.")
        return parquet_path

    try:
        parquet_count = pl.scan_parquet(parquet_path).select(pl.len()).collect().item()
        csv_count = (
            pl.scan_csv(csv_path, ignore_errors=True).select(pl.len()).collect().item()
        )
    except Exception as exc:
        logger.error(f"Failed to count rows: {exc}")
        raise

    if csv_count == parquet_count:
        logger.info(f"Row counts match ({csv_count}); deleting {csv_path.name}.")
        try:
            csv_path.unlink()
        except Exception as exc:
            logger.error(f"Failed to delete CSV {csv_path}: {exc}")
            raise
    else:
        logger.warning(
            f"Row count mismatch! CSV: {csv_count}, Parquet: {parquet_count}. Deleting both files."
        )
        try:
            csv_path.unlink()
        except Exception as exc:
            logger.error(f"Failed to delete CSV {csv_path}: {exc}")
            raise
        try:
            parquet_path.unlink()
        except Exception as exc:
            logger.error(f"Failed to delete Parquet {parquet_path}: {exc}")
            raise

    return parquet_path


@task(log_prints=True)
def upload_to_gcs(path: Path) -> None:
    """Upload file to GCS bucket."""
    logger = get_run_logger()

    if not all([project_id, creds, bucket_name]):
        raise ValueError(
            "GCP_PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS, and GCP_GCS_BUCKET are required"
        )

    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    destination_blob_name = f"ny_service_requests/{path.name}"
    blob = bucket.blob(destination_blob_name)

    logger.info(f"Uploading {path.name} to {bucket_name}")

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"ny_service_requests/{path.name}")
    blob.upload_from_filename(str(path))
    logger.info(f"Successfully uploaded: {path.name}")