# Ingestion Service Dockerfile

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy shared packages first
COPY packages/common /app/packages/common
COPY packages/schemas /app/packages/schemas

# Install shared packages
RUN pip install --no-cache-dir -e /app/packages/common
RUN pip install --no-cache-dir -e /app/packages/schemas

# Copy service files
COPY services/ingestion/pyproject.toml /app/
COPY services/ingestion/src /app/src

# Install service dependencies
RUN pip install --no-cache-dir \
    aiohttp==3.9.0 \
    kafka-python==2.0.2 \
    pyyaml==6.0 \
    pydantic==2.5.0 \
    pydantic-settings==2.1.0

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    LOG_LEVEL=INFO

# Run the service
CMD ["python", "-m", "src.main"]
