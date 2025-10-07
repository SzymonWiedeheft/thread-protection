# Development Setup Guide

## Prerequisites

- **Python 3.11+** (3.12 recommended for development, 3.11 for Spark containers)
- **Poetry 1.8+** for dependency management (Docker images pin 1.8.5)
- **Docker Desktop** (20.10+) with Docker Compose
- **Make** (optional, for convenience commands)
- **50GB+ free disk space** for Docker images and data

## Initial Setup

### 1. Install Python Dependencies

Install all packages and services using Poetry:

```bash
# Install all packages and services
make install

# This installs:
# - packages/common
# - packages/schemas
# - packages/monitoring
# - services/ingestion
# - services/streaming
# - services/orchestration
```

Or install packages individually:

```bash
# Install individual packages
cd packages/common && poetry install
cd packages/schemas && poetry install
cd packages/monitoring && poetry install

# Install services
cd services/ingestion && poetry install
cd services/streaming && poetry install
cd services/orchestration && poetry install

# Install integration tests
cd tests && poetry install
```

### 2. Build Docker Images & Start Services

Build the Docker images and start all services:

```bash
# Build images and start all services
make start

# This will:
# 1. Build thread-protection-ingestion:latest
# 2. Build thread-protection-streaming:latest
# 3. Start all Docker Compose services
# 4. Configure Airflow variables
```

> `make start` orchestrates `docker compose -f infrastructure/docker/docker-compose.yml up -d`. If you keep a `docker-compose.override.yml`, ensure it preserves the `PYTHONPATH` and monitoring settings introduced in the base file (or remove the override) before starting the stack.

### 3. Verify Services

Check that all services are running:

```bash
# Check Docker service status
make status

# Or use docker compose directly
cd infrastructure/docker && docker compose ps
```

### 4. Access Service UIs

All services should now be accessible:

| Service | URL | Credentials | Description |
|---------|-----|-------------|-------------|
| **Grafana** | http://localhost:3000 | admin/admin | Monitoring dashboards |
| **Prometheus** | http://localhost:9090 | -           | Metrics & alerts |
| **Prometheus Pushgateway** | http://localhost:9091 | -           | Ingestion metrics push endpoint |
| **Airflow** | http://localhost:8082 | admin/admin | Workflow orchestration |
| **Spark Master** | http://localhost:8080 | -           | Spark cluster UI |
| **Trino** | http://localhost:8081 | admin       | SQL query interface |

### 5. Verify Data Pipeline

Check that data is flowing through the pipeline:

```bash
# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Expected topics:
# - malware_domains
# - ads_trackers_domains

# Check Delta Lake tables in Trino
# Visit http://localhost:8081 or use CLI:
docker exec trino trino --execute "SHOW SCHEMAS IN hive"
# Expected: bronze, silver, gold, default

docker exec trino trino --execute "SHOW TABLES IN hive.bronze"
# Expected: domains

# Check monitoring metrics in Prometheus
# Visit http://localhost:9090 and query:
# - thread_protection_ingestion_pipeline_health
# - metrics_master_aliveWorkers_Value
```

## Services Overview

### Infrastructure Services

#### Kafka & Zookeeper
- **Kafka**: Message broker for streaming data
  - Internal: `kafka:29092`
  - External: `localhost:9092`
- **Zookeeper**: Kafka coordination service
  - Port: `2181`

#### Spark Cluster
- **Spark Master**: Cluster coordinator
  - UI: http://localhost:8080
  - Internal: `spark://spark-master:7077`
- **Spark Workers**: Compute nodes (2 workers)
  - `spark-worker`: General purpose
  - `spark-worker-trino`: Dedicated for Trino queries

#### Delta Lake & Trino
- **PostgreSQL Metastore** (`postgres-metastore`): Hive Metastore backend
  - Port: `5433`
- **Hive Metastore**: Metadata service for Delta tables
  - Thrift: `thrift://hive-metastore:9083`
- **Trino**: SQL query engine
  - UI: http://localhost:8081
  - Catalogs: `hive`, `delta`

#### Orchestration
- **Airflow Webserver**: DAG UI
  - http://localhost:8082
  - Credentials: admin/admin
- **Airflow Scheduler**: DAG execution
- **PostgreSQL (Airflow)**: Airflow metadata DB
  - Port: `5432`

#### Monitoring Stack
- **Prometheus**: Metrics collection
  - http://localhost:9090
  - Scrapes Spark, Airflow metrics
- **Prometheus Pushgateway**: Metric push endpoint
  - http://localhost:9091
  - Used by monitoring DAG
- **Grafana**: Visualization
  - http://localhost:3000
  - Dashboards: Spark Master, Pipeline Health

### Application Services

#### Ingestion Service
- **Container**: Run by Airflow DAG
- **Purpose**: Fetch blocklists and publish to Kafka
- **Topics**: `malware_domains`, `ads_trackers_domains`

#### Streaming Services
- **Bronze Stream**: Kafka → Bronze Delta Lake
  - Container: `bronze-stream`
  - Table: `bronze.domains`
  - Auto-registers `bronze.domains` in Hive Metastore if missing
- **Silver Stream**: Bronze → Silver Delta Lake
  - Container: `silver-stream`
  - Table: `silver.domains`
  - Auto-registers `silver.domains` in Hive Metastore if missing
- **Gold Stream**: Silver → Gold Delta Lake
  - Container: `gold-stream`
  - Table: `gold.domains`
  - Auto-registers `gold.domains` in Hive Metastore if missing

## Monitoring & Observability

### Grafana Dashboards

Two dashboards are auto-provisioned:

**1. Spark Master Overview** (`Spark` folder)
- Alive workers count
- Total registered cores
- Worker free memory
- JVM heap usage

**2. Ingestion Pipeline Health** (`Spark` folder)
- Overall pipeline health (1=healthy, 0=unhealthy)
- Component health table (Kafka, Spark, Bronze, Silver, Gold)
- Failed checks count over time

### Prometheus Datasource

The Prometheus datasource is automatically configured in Grafana on startup via provisioning files in `infrastructure/docker/configs/grafana/provisioning/datasources/`.

If the datasource is missing:
1. Check `docker logs grafana` for provisioning errors
2. Verify file exists: `infrastructure/docker/configs/grafana/provisioning/datasources/datasources.yml`
3. Restart Grafana: `docker compose restart grafana`

### Prometheus Metrics Overview

**Spark servlets (scraped by Prometheus)**
- `metrics_master_aliveWorkers_Value` – Spark master view of active workers
- `metrics_worker_memoryFree_Value` – Free memory reported by each worker
- JVM gauges (heap usage, executor metrics)

**Airflow monitoring DAG (pushed via Pushgateway)**
- `thread_protection_ingestion_component_health{component="bronze"}` – Per-component health (0/1)
- `thread_protection_ingestion_pipeline_health` – Overall pipeline health (0/1)
- `thread_protection_ingestion_pipeline_failed_checks_total` – Count of failed checks
- `thread_protection_ingestion_pipeline_last_run_timestamp` – Epoch timestamp of the latest DAG run

Prometheus targets are listed at http://localhost:9090/targets and should show `prometheus`, `spark-master`, `spark-worker`, and `pushgateway` in the UP state.

### Updating Metrics

After modifying Spark or monitoring code:

```bash
# Restart Spark containers to reload metrics config
cd infrastructure/docker
docker compose restart spark-master spark-worker spark-worker-trino

# Restart Airflow to reload monitoring DAG
docker compose restart airflow-webserver airflow-scheduler

# Restart the Pushgateway if metric labels or naming changed
docker compose restart prometheus-pushgateway

# Trigger monitoring DAG to update metrics
docker exec airflow-webserver airflow dags trigger monitoring_dag
```

## Development Workflow

### Running Tests

```bash
# Run all tests (unit + integration)
make test

# Run only unit tests
make test-unit

# Run only integration tests (requires Docker services)
make test-integration

# Run specific service tests
cd services/ingestion && poetry run pytest
cd services/streaming && poetry run pytest
```

### Code Quality

```bash
# Format all code with Black
make format

# Lint all code with Ruff
make lint

# Run both
make format && make lint
```

### Working with Individual Services

Each service can be developed independently:

```bash
# Ingestion service
cd services/ingestion
poetry install
poetry run pytest
poetry run python -m src.main --help

# Streaming service
cd services/streaming
poetry install
poetry run pytest

# Orchestration (Airflow)
cd services/orchestration
poetry install
poetry run pytest
```

## Data Management

### Delta Lake Tables

All tables are in `/data/delta/`:

```bash
# Check table structure
ls -la data/delta/bronze/domains/
ls -la data/delta/silver/domains/
ls -la data/delta/gold/domains/

# Each has:
# - _delta_log/       # Transaction log
# - parquet files     # Data files
# - partition dirs    # ingestion_date=YYYY-MM-DD
```

### Querying Data

Use Trino to query Delta tables:

```bash
# Interactive SQL shell
docker exec -it trino trino

# Or execute single query
docker exec trino trino --execute "SELECT COUNT(*) FROM hive.bronze.domains"
```

Example queries:

```sql
-- List all catalogs
SHOW CATALOGS;

-- List schemas in hive catalog
SHOW SCHEMAS IN hive;

-- Check table in bronze
SELECT * FROM hive.bronze.domains
WHERE ingestion_date = CURRENT_DATE
LIMIT 10;

-- Aggregation from gold
SELECT * FROM hive.gold.domains;
```

### Cleaning Data

```bash
# Clean all data (Delta tables, checkpoints)
make clean-data

# This removes:
# - data/
# - checkpoints/
# - spark-warehouse/
# - metastore_db/
```

## Environment Variables

Key environment variables used by services:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Spark
SPARK_MASTER=spark://spark-master:7077

# Delta Lake paths
DELTA_BRONZE_PATH=/data/delta/bronze/domains
DELTA_SILVER_PATH=/data/delta/silver/domains
DELTA_GOLD_PATH=/data/delta/gold/domains

# Hive Metastore
HIVE_METASTORE_URI=thrift://hive-metastore:9083

# Monitoring
PROMETHEUS_PUSHGATEWAY_URL=http://prometheus-pushgateway:9091
PYTHONPATH=/opt/packages/monitoring/src:$PYTHONPATH  # Airflow containers
```

These are configured in `infrastructure/docker/docker-compose.yml` and `scripts/setup-airflow-vars.sh`.
