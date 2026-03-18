"""NYC 311 Service Requests - Extract and Load Orchestration"""

import datetime
from pydantic import BaseModel, Field, model_validator
from typing import Optional
from prefect import flow, get_run_logger
from extract.web_to_gcs import download_data, format_to_parquet, upload_to_gcs

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
def extract_load_flow(year: int, month: int) -> Optional[str]:
    """
    Extract NYC 311 data, convert to Parquet, and load to GCS.
    """
    logger = get_run_logger()
    logger.info(f"Starting ETL for {year}-{month:02d}")

    csv_path = download_data(year, month)
    parquet_path = format_to_parquet(csv_path)

    # upload_to_gcs(parquet_path)

    logger.info(f"ETL completed for {year}-{month:02d}")
    return str(parquet_path) if parquet_path else None


@flow(name="NYC 311 Annual Backfill")
def backfill_year_flow(year: int) -> None:
    """Backfill data for an entire year."""
    
    year_params = YearParams(year=year)
    now = datetime.datetime.now()
    max_month = now.month if year_params.year == now.year else 12

    for month in range(1, max_month + 1):
        extract_load_flow(year=year_params.year, month=month)


if __name__ == "__main__":
    backfill_year_flow(year=2024)