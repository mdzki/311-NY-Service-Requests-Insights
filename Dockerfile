FROM python:3.13.12-slim

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*


COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app


COPY pyproject.toml uv.lock ./
RUN uv sync --frozen


COPY . .


ENV PATH="/app/.venv/bin:$PATH"
