# Multi-stage build for minimal image size
FROM python:3.12-slim AS builder

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (better layer caching)
COPY pyproject.toml uv.lock ./

# Install dependencies into a virtual environment
RUN uv sync --frozen --no-dev --no-install-project

# Production stage
FROM python:3.12-slim

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY . .

# Use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Default port (override with -e PORT=xxxx)
ENV PORT=8080

EXPOSE 8080

# Run with uvicorn (--no-access-log since we use structured logging)
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port $PORT --no-access-log"]
