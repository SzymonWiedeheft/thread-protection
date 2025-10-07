FROM spark:3.5.0-scala2.12-java11-python3-ubuntu

USER root

# Install base tooling required for dependency management and build Python 3.11
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    libbz2-dev \
    libffi-dev \
    liblzma-dev \
    libncursesw5-dev \
    libreadline-dev \
    libsqlite3-dev \
    libssl-dev \
    tk-dev \
    wget \
    xz-utils \
    zlib1g-dev \
 && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz -o /tmp/Python-3.11.9.tgz && \
    tar -xzf /tmp/Python-3.11.9.tgz -C /tmp && \
    cd /tmp/Python-3.11.9 && \
    ./configure --enable-optimizations --with-ensurepip=install && \
    make -j"$(nproc)" && \
    make altinstall && \
    ln -sf /usr/local/bin/python3.11 /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/python3.11 /usr/local/bin/python && \
    python3 -m pip install --no-cache-dir --upgrade pip && \
    rm -rf /tmp/Python-3.11.9 /tmp/Python-3.11.9.tgz

# Install Poetry
ARG POETRY_VERSION=2.2.1
RUN curl -sSL https://install.python-poetry.org | python3 - --version "$POETRY_VERSION" && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Disable Poetry virtualenv creation so dependencies land in system site-packages
ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VIRTUALENVS_IN_PROJECT=false

# Set working directory
WORKDIR /app

# Copy package dependencies first (for better layer caching)
COPY packages/common /app/packages/common
COPY packages/schemas /app/packages/schemas

# Copy streaming service
COPY services/streaming /app/services/streaming

# Install dependencies for packages
WORKDIR /app/packages/common
RUN --mount=type=cache,target=/root/.cache/pip rm -rf .venv && poetry install --only main --no-interaction --no-ansi

WORKDIR /app/packages/schemas
RUN --mount=type=cache,target=/root/.cache/pip rm -rf .venv && poetry install --only main --no-interaction --no-ansi

# Install streaming service dependencies
WORKDIR /app/services/streaming
RUN --mount=type=cache,target=/root/.cache/pip rm -rf .venv && poetry install --only main --no-interaction --no-ansi

# Use bundled PySpark distribution to ensure driver matches installed library version
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PATH="$SPARK_HOME/bin:$PATH" \
    PYSPARK_PYTHON=python3

# Set Python path
ENV PYTHONPATH=/app/services/streaming:$PYTHONPATH

# Entry point
ENTRYPOINT ["python", "-m", "src.main"]
