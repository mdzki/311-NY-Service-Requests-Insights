"""NYC 311 Service Requests - Extract and Load Orchestration"""

import datetime
from pydantic import BaseModel, Field, model_validator
from typing import Optional
from prefect import flow, get_run_logger
from extract.web_to_gcs import (
    check_if_exists_in_gcs,
    download_data,
    format_to_parquet,
    validate_parquet_with_csv_and_cleanup,
    upload_to_gcs,
)


# Validation models for flow parameters
class YearParams(BaseModel):
    year: int = Field(..., ge=2020, le=2026)


class MonthParams(YearParams):
    month: int = Field(..., ge=1, le=12)

    @model_validator(mode="after")
    def check_future_month(self) -> "MonthParams":
        now = datetime.datetime.now()
        if self.year == now.year and self.month > now.month:
            raise ValueError(f"Max valid month for {self.year} is {now.month}")
        return self


@flow(name="NYC 311 Monthly Extract and Load")
def extract_load_flow(year: int, month: int, overwrite: bool = False, erase_local_files: bool = True) -> None:
    """
    Extract NYC 311 data, convert to Parquet, and load to GCS.
    """
    logger = get_run_logger()
    logger.info(f"Starting ETL for {year}-{month:02d}")

    now = datetime.datetime.now()
    is_current_month = (year == now.year and month == now.month)

    if not overwrite and check_if_exists_in_gcs(year, month) and not is_current_month:
        logger.info(f"File for {year}-{month:02d} already exists in GCS. Skipping.")
    else:
        logger.info(f"Overwriting existing file for {year}-{month:02d} in GCS.")
        csv_path = download_data(year, month)
        parquet_path = format_to_parquet(csv_path)
        validated_parquet_path = validate_parquet_with_csv_and_cleanup(parquet_path, erase_local_files)
        upload_to_gcs(validated_parquet_path)
        logger.info(f"ETL completed for {year}-{month:02d}")


@flow(name="NYC 311 Annual Backfill")
def backfill_year_flow(year: int, overwrite: bool = False, erase_local_files: bool = True) -> None:
    """Backfill data for an entire year."""

    year_params = YearParams(year=year)
    now = datetime.datetime.now()
    max_month = now.month if year_params.year == now.year else 12

    for month in range(1, max_month + 1):
        extract_load_flow(year=year_params.year, month=month, overwrite=overwrite, erase_local_files=erase_local_files)


@flow(name="NYC 311 Total Backfill 2020-2026")
def backfill_full_dataset(overwrite: bool = False, erase_local_files: bool = True) -> None:
    """Backfill data for an entire dataset."""

    now = datetime.datetime.now()
    
    for year in range(2020, now.year + 1):
        backfill_year_flow(year=year, overwrite=overwrite, erase_local_files=erase_local_files)


if __name__ == "__main__":
    backfill_year_flow(year=2024, overwrite=False, erase_local_files=True)
