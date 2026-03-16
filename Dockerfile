FROM python:3.14.3-slim

# Install uv to manage dependencies
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set the working directory inside the container
WORKDIR /app

# Copy dependency files first (better caching)
COPY pyproject.toml uv.lock ./

# Install dependencies (frozen ensures exact versions)
RUN uv sync --frozen

# Copy the rest of your application code
COPY . .

# Add the virtualenv to the PATH so 'prefect' and 'python' work correctly
ENV PATH="/app/.venv/bin:$PATH"

# The worker will be started via docker-compose command