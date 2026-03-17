"""NYC 311 Service Requests - Extract and Load Orchestration"""

from prefect import flow
from extract.web_to_gcs import download_data, format_to_parquet, upload_to_gcs


@flow(name="NYC 311 Monthly Extract and Load")
def extract_load_flow(year: int = 2024, month: int = 1):
    """Extract NYC 311 data and load to GCS."""
    csv_path = download_data(year, month)
    parquet_path = format_to_parquet(csv_path)
    upload_to_gcs(parquet_path)


@flow(name="NYC 311 Annual Backfill")
def backfill_year_flow(year: int):
    """Backfill data for an entire year."""
    for month in range(1, 13):
        extract_load_flow(year, month)


if __name__ == "__main__":
    extract_load_flow()
