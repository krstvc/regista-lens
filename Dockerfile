FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (cached layer)
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project

# Copy source and install project
COPY . .
RUN uv sync --no-dev

# Make venv binaries available without "uv run"
ENV PATH="/app/.venv/bin:$PATH"

# Dagster home — shared between webserver and daemon via volume
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME && \
    echo "{}" > $DAGSTER_HOME/dagster.yaml
