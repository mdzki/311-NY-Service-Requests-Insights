# 311-NY-Service-Requests-Insights

Data pipeline for NYC 311 service requests using Prefect and GCP.

## Overview

Extracts NYC 311 data from the API, converts to Parquet, and loads to Google Cloud Storage. 
Uses Prefect for orchestration and dbt for transformations.

## Setup

```bash
pip install -r requirements.txt  # or use uv sync
```

## Configuration

Copy the example environment file and update with your values:
```bash
cp .env.example .env
```

Set environment variables:
```bash
export GCP_PROJECT_ID="your-project"
export GCP_BUCKET="your-bucket"
export PREFECT_API_URL="http://localhost:4200"
```

## Running

```bash
# Local execution - monthly data
python main.py


# Via Prefect with Docker
docker-compose up
```

## Project Structure

```
main.py                 # Extract & load pipeline
extract/               # Legacy extraction code
data/                  # Local data files
docker-compose.yml     # Prefect setup
Dockerfile
pyproject.toml
```

## Tech Stack

- **Prefect**: Workflow orchestration
- **Polars**: Data processing
- **GCP**: Cloud storage
- **dbt**: Transformations