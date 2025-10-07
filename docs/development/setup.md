# Development Setup

## Prerequisites

- Python 3.12
- Poetry 1.7+in
- Docker & Docker Compose
- Make (optional, for convenience)

## Initial Setup

### 1. Install Dependencies

Install all packages:
```bash
make install
```

Or install individual packages:
```bash
make install-common
make install-ingestion
make install-api
# etc...
```

### 2. Configure Environment

Copy the example environment file:
```bash
cp .env.example .env
```

Edit `.env` with your configuration.

### 3. Start Local Services

Start all services with Docker Compose:
```bash
make docker-up
```

This starts:
- Kafka & Zookeeper
- Redis
- PostgreSQL (for Airflow)
- Prometheus & Grafana
- All application services

### 4. Verify Setup

Check service health:
```bash
# API
curl http://localhost:8000/health

# Prometheus
open http://localhost:9090

# Grafana
open http://localhost:3000

# Airflow
open http://localhost:8080
```

## Monitoring

Spark master and worker containers expose Prometheus metrics via `/metrics/*/prometheus` after the stack starts. Prometheus scrapes these endpoints automatically, and Grafana provisions a `Spark Master Overview` dashboard with common health signals (alive workers, registered cores, and worker memory headroom).

After changing Prometheus or Spark configs, restart the Spark containers so they pick up the `metrics.properties` file:
```bash
docker compose -f infrastructure/docker/docker-compose.yml restart spark-master spark-worker spark-worker-trino
```

Then reload Prometheus and verify the new scrape targets are healthy:
```bash
curl -X POST http://localhost:9090/-/reload
open http://localhost:9090/targets
```

Grafana at `http://localhost:3000` (admin/admin) loads a `Spark Master Overview` dashboard under the `Spark` folder so you can confirm the metrics stream without additional configuration.

The monitoring stack also provisions a Prometheus Pushgateway (`http://localhost:9091`) that the Airflow monitoring DAG uses to publish ingestion health snapshots. Restart `airflow-webserver`, `airflow-scheduler`, and `prometheus-pushgateway` whenever you touch `monitoring_dag.py` metrics code so the updated package is reinstalled:
```bash
docker compose -f infrastructure/docker/docker-compose.yml restart airflow-webserver airflow-scheduler prometheus-pushgateway
```

Look for the `Ingestion Pipeline Health` dashboard in Grafana’s `Spark` folder to visualize the overall pipeline status, component-level health gauges, and the latest failed check counts sourced from the monitoring DAG.

## Development Workflow

### Running Tests

Run all tests:
```bash
make test
```

Run package-specific tests:
```bash
make test-ingestion
make test-api
```

### Code Formatting

Format all code:
```bash
make format
```

### Linting

Run linters:
```bash
make lint
```

### Clean Build Artifacts

```bash
make clean
```

## Package Structure

The monorepo contains the following packages:

- **common**: Shared utilities and models
- **ingestion**: Data fetching and Kafka publishing
- **dlt-pipelines**: Databricks Delta Live Tables
- **api**: FastAPI REST service
- **monitoring**: Observability and metrics
- **airflow**: Workflow orchestration

## Working with Individual Packages

Each package can be developed independently:

```bash
cd packages/ingestion
poetry install
poetry run pytest
poetry run python -m thread_protection_ingestion.main
```

## Troubleshooting

### Port Conflicts

If ports are already in use, modify `docker-compose.yml`:
- API: 8000
- Airflow: 8080
- Prometheus: 9090
- Grafana: 3000
- Kafka: 9092
- Redis: 6379

### Poetry Issues

Clear cache:
```bash
poetry cache clear . --all
poetry install
```
