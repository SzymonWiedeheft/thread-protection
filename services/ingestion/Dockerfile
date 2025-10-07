# Ingestion Service Dockerfile

FROM python:3.11-slim AS wheels-builder

WORKDIR /build

# Prepare build tooling
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --upgrade pip setuptools wheel

# Build wheels for shared libraries
COPY packages/common /build/packages/common
COPY packages/schemas /build/packages/schemas
RUN pip wheel /build/packages/common -w /dist
RUN pip wheel /build/packages/schemas -w /dist

FROM python:3.11-slim

WORKDIR /app

# Runtime dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install shared libraries from wheels
COPY --from=wheels-builder /dist /wheels
RUN pip install --no-cache-dir /wheels/common-*.whl /wheels/schemas-*.whl

# Copy service source
COPY services/ingestion/pyproject.toml /app/
COPY services/ingestion/src /app/src

# Install service dependencies
RUN pip install --no-cache-dir \
    aiohttp==3.9.0 \
    kafka-python==2.0.2 \
    pyyaml==6.0 \
    pydantic==2.5.0 \
    pydantic-settings==2.1.0 \
    tenacity==8.2.0

# Ensure src is importable when running python -m src.main
ENV PYTHONPATH=/app/src:/app

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# Default environment variables
ENV PYTHONUNBUFFERED=1 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    LOG_LEVEL=INFO

CMD ["python", "-m", "src.main"]
