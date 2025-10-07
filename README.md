# Thread Protection - Harmful Domain Data Pipeline

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![Spark 3.5](https://img.shields.io/badge/spark-3.5-orange.svg)](https://spark.apache.org/)
[![Delta Lake 3.2](https://img.shields.io/badge/delta--lake-3.2-green.svg)](https://delta.io/)
[![Monorepo](https://img.shields.io/badge/monorepo-structure-blue.svg)](https://monorepo.tools/)

## Real-Time Lakehouse Architecture with Spark, Kafka, Delta Lake, and Trino

A production-ready data pipeline for collecting, normalizing, and querying harmful domain information from multiple heterogeneous sources in real-time (sub-minute latency). Built on the medallion architecture (Bronze → Silver → Gold) using Delta Lake as the storage layer with Trino for interactive querying.

## 🏗️ Architecture

### Data Flow
```
HTTP Sources → Ingestion Service → Kafka Topics →
Spark Streaming (Bronze → Silver → Gold) →
Delta Lake (Medallion Architecture) →
Hive Metastore ← Trino (SQL Queries)
```

### Medallion Architecture
- **Bronze Layer**: Raw data from Kafka with full lineage tracking
- **Silver Layer**: Cleaned, validated, and deduplicated data
- **Gold Layer**: Aggregated metrics and optimized for analytics

### Monorepo Structure
```
thread-protection/
├── services/              # Microservices
│   ├── ingestion/         # HTTP fetching & Kafka production
│   ├── streaming/         # Spark Structured Streaming (Bronze/Silver/Gold)
│   └── orchestration/     # Airflow DAGs & custom operators
│
├── packages/              # Shared libraries
│   ├── common/            # Utilities, logging, exceptions
│   ├── schemas/           # Pydantic models, Delta schemas, Kafka messages
│   └── monitoring/        # Prometheus metrics & health checks
│
├── infrastructure/        # Infrastructure as Code
│   ├── docker/            # Docker Compose configs & service configs
│   │   ├── configs/       # Spark, Trino, Prometheus, Grafana configs
│   │   └── docker-compose.yml
│
├── tests/                 # Integration & performance tests
├── scripts/               # Build and deployment scripts
└── docs/                  # Documentation
```

## 🚀 Quick Start

### Prerequisites
- **Docker Desktop** (20.10+) with Docker Compose
- **Python 3.11+** (3.12 recommended for development)
- **Make** (optional, for convenience)
- **50GB+ free disk space** (for Docker images and data)

### Installation

```bash
# 1. Clone repository
git clone <repository-url>
cd thread-protection

# 2. Install all packages and services
make install

# 3. Build Docker images and start all services
make start

# 4. Check service status
make status
```

> `make start` orchestrates `docker compose -f infrastructure/docker/docker-compose.yml up -d`. If you maintain a custom `docker-compose.override.yml`, ensure it keeps the new `PYTHONPATH` and monitoring settings (or remove it) so Airflow can import the `monitoring` package.

### Access Services

After starting, services are available at:

| Service | URL | Credentials | Description |
|---------|-----|-------------|-------------|
| **Grafana** | http://localhost:3000 | admin/admin | Monitoring dashboards |
| **Prometheus** | http://localhost:9090 | -           | Metrics & monitoring |
| **Prometheus Pushgateway** | http://localhost:9091 | -           | Ingestion metrics push endpoint |
| **Airflow** | http://localhost:8082 | admin/admin | Workflow orchestration |
| **Spark Master UI** | http://localhost:8080 | -           | Spark cluster status |
| **Trino** | http://localhost:8081 | admin       | SQL query interface |
| **Kafka** | localhost:9092 | -           | Message broker |

### Makefile Commands

```bash
# Setup & Start
make install              # Install all packages/services (Poetry)
make start                # Build images & start Docker services
make stop                 # Stop Docker services
make restart              # Restart all Docker services
make status               # Check service status

# Development
make test                 # Run all tests (unit + integration)
make test-unit            # Run unit tests only
make test-integration     # Run integration tests only
make lint                 # Lint all code
make format               # Format all code with Black

# Cleanup
make clean                # Clean build artifacts
make clean-data           # Clean data directories (Delta, checkpoints)
make clean-venv           # Remove all virtual environments
```

## 📊 Services

### Ingestion Service (`services/ingestion/`)
**Purpose**: Fetch domain blocklists from HTTP sources and publish to Kafka

**Features**:
- Async HTTP fetching with retry logic
- Multi-format parsers (hosts file, adblock, domains)
- Domain validation and enrichment
- Kafka producer with batching

**Data Sources**:
- Malware domains (URLhaus, abuse.ch)
- Ad/tracker domains (EasyList, hosts files)

### Streaming Service (`services/streaming/`)
**Purpose**: Real-time Spark Structured Streaming for the medallion architecture

**Streams**:
- **Bronze Stream**: Kafka → Bronze Delta Lake (raw ingestion)
- **Silver Stream**: Bronze → Silver Delta Lake (cleaning, validation, deduplication)
- **Gold Stream**: Silver → Gold Delta Lake (aggregations, metrics)

**Features**:
- Auto-registers Delta tables with the Hive metastore on startup
- Change Data Feed (CDF) enabled
- Auto-optimization and compaction
- Prometheus metrics exposed via Spark metrics system (master & workers)

### Orchestration Service (`services/orchestration/`)
**Purpose**: Airflow-based workflow orchestration and monitoring

**DAGs**:
- **domain_ingestion_dag**: Scheduled data ingestion (daily)
- **monitoring_dag**: Health checks and metrics collection (every 15 min)

**Custom Operators**:
- `IngestionOperator`: Run ingestion service
- `KafkaHealthCheckOperator`: Kafka broker & topic health
- `DataFreshnessCheckOperator`: Delta table freshness checks
- `SparkJobCheckOperator`: Spark streaming job health

## 📦 Packages

### Common (`packages/common/`)
- Shared utilities and helper functions
- Structured logging (structlog)
- Custom exception classes
- Retry logic and decorators

### Schemas (`packages/schemas/`)
- **Pydantic Models**: Domain data models
- **Kafka Schemas**: Message formats for topics
- **Delta Schemas**: Bronze/Silver/Gold table schemas
- **Validation Rules**: Domain validation logic

### Monitoring (`packages/monitoring/`)
- **Prometheus Metrics**: Custom metrics via Pushgateway
- **Health Checks**: Component health status
- **Alerting**: Health snapshot publishing for DAGs

## 🗄️ Data Architecture

### Delta Lake Tables

All tables are stored in Delta Lake format with Hive Metastore registration:

| Table | Schema | Partition Key | Description |
|-------|--------|---------------|-------------|
| `bronze.domains` | Raw Kafka messages | `ingestion_date` | Unprocessed domains with Kafka metadata |
| `silver.domains` | Cleaned domains | `ingestion_date` | Validated, deduplicated domains |
| `gold.domains` | Aggregated metrics | None | Category counts, statistics |

### Querying with Trino

```sql
-- Connect to Trino at localhost:8081
-- Use catalog: hive

-- Query bronze layer
SELECT * FROM hive.bronze.domains
WHERE ingestion_date = CURRENT_DATE
LIMIT 10;

-- Query silver layer (clean data)
SELECT category, COUNT(*) as count
FROM hive.silver.domains
GROUP BY category;

-- Query gold layer (aggregations)
SELECT * FROM hive.gold.domains;
```

## 📈 Monitoring & Observability

### Grafana Dashboards

Two pre-configured dashboards are available at http://localhost:3000:

1. **Spark Master Overview** (`/d/spark-master-overview`)
   - Alive workers count
   - Total registered cores
   - Worker memory usage
   - JVM heap metrics

2. **Ingestion Pipeline Health** (`/d/ingestion-pipeline-health`)
   - Overall pipeline health status
   - Component health (Kafka, Spark, Bronze, Silver, Gold)
   - Failed health checks count
   - Last monitoring run timestamp

### Prometheus Metrics

**Spark Metrics** (scraped from Spark master/worker servlets):
- `metrics_master_aliveWorkers_Value` – Active worker count
- `metrics_worker_memoryFree_Value` – Worker memory headroom
- JVM gauges (heap usage, executor metrics)

**Pipeline Metrics** (pushed from Airflow monitoring DAG via Pushgateway):
- `thread_protection_ingestion_component_health` – Per-component health (0/1)
- `thread_protection_ingestion_pipeline_health` – Overall pipeline health (0/1)
- `thread_protection_ingestion_pipeline_failed_checks_total` – Failed check count
- `thread_protection_ingestion_pipeline_last_run_timestamp` – Last DAG execution time (epoch)

## 🧪 Testing

### Run Tests

```bash
# All tests
make test

# Unit tests only
make test-unit

# Integration tests (requires Docker services)
make test-integration
```

### Test Structure

```
tests/
├── integration/           # End-to-end integration tests
│   ├── test_ingestion_kafka.py
│   ├── test_streaming_bronze.py
│   └── test_streaming_silver.py
└── fixtures/             # Shared test fixtures
```

## 🔧 Configuration

### Environment Variables

Key environment variables (set in docker-compose.yml):

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Spark
SPARK_MASTER=spark://spark-master:7077

# Delta Lake paths
DELTA_BRONZE_PATH=/data/delta/bronze/domains
DELTA_SILVER_PATH=/data/delta/silver/domains
DELTA_GOLD_PATH=/data/delta/gold/domains

# Monitoring
PROMETHEUS_PUSHGATEWAY_URL=http://prometheus-pushgateway:9091
PYTHONPATH=/opt/packages/monitoring/src:$PYTHONPATH  # Airflow + monitoring DAG
```

### Airflow Variables

Configured automatically via `scripts/setup-airflow-vars.sh`:

```python
kafka_servers = "kafka:29092"
spark_master = "spark://spark-master:7077"
bronze_path = "/data/delta/bronze/domains"
silver_path = "/data/delta/silver/domains"
gold_path = "/data/delta/gold/domains"
```

## 📚 Documentation

- [Development Setup](docs/development/setup.md) - Local development guide

## 🐛 Troubleshooting

### Services won't start
```bash
# Check disk space (need 50GB+)
df -h

# Clean Docker resources
docker system prune -a --volumes -f

# Restart from scratch
make stop
make clean-data
make start
```

### Dashboards are empty in Grafana
- Datasource is auto-provisioned on startup (check http://localhost:3000/datasources)
- If Spark panels are empty, restart Spark containers to reload `metrics.properties`
  ```bash
  cd infrastructure/docker
  docker compose restart spark-master spark-worker spark-worker-trino
  ```
- Trigger `monitoring_dag` in Airflow to publish ingestion metrics:
  ```bash
  docker exec airflow-webserver airflow dags trigger monitoring_dag
  ```
- Confirm Prometheus targets (`http://localhost:9090/targets`) show `spark-master`, `spark-worker`, and `pushgateway` as UP

### ModuleNotFoundError: No module named 'monitoring'
- Remove any `docker-compose.override.yml` that reintroduces older environment variables
- Recreate Airflow services so the base `PYTHONPATH` setting takes effect:
  ```bash
  cd infrastructure/docker
  docker compose up -d airflow-init airflow-webserver airflow-scheduler
  ```

### Trino can't see tables
- Tables are registered in Hive Metastore by Spark streaming jobs
- Verify Spark reports `hive` catalog implementation:
  ```bash
  docker exec bronze-stream python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(spark.conf.get("spark.sql.catalogImplementation"))
PY
  ```
- Check streaming containers are running: `docker ps | grep stream`
- Review Hive Metastore and backing Postgres logs for errors:
  ```bash
  docker logs hive-metastore | tail -20
  docker logs postgres-metastore | tail -20
  ```
- Restart the streaming jobs to force table registration if needed:
  ```bash
  cd infrastructure/docker
  docker compose restart bronze-stream silver-stream gold-stream
  ```
