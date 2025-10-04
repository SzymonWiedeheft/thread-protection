# Nord Security - Harmful Domain Data Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![Spark 3.5](https://img.shields.io/badge/spark-3.5-orange.svg)](https://spark.apache.org/)
[![Monorepo](https://img.shields.io/badge/monorepo-structure-blue.svg)](https://monorepo.tools/)

## Real-Time Streaming Architecture with Spark, Kafka, and Delta Lake

A production-ready data pipeline for collecting, normalizing, and querying harmful domain information from multiple heterogeneous sources in real-time (sub-minute latency).

## 🏗️ Monorepo Architecture

```
nord-security-pipeline/
├── services/          # Microservices
│   ├── ingestion/     # HTTP fetching & Kafka production
│   ├── streaming/     # Spark Structured Streaming
│   └── orchestration/ # Airflow DAGs
│
├── packages/          # Shared libraries
│   ├── common/        # Utilities, logging, exceptions
│   ├── schemas/       # Data models and schemas
│   └── monitoring/    # Metrics and health checks
│
├── infrastructure/    # Infrastructure as Code
│   ├── docker/        # Docker Compose configs
│   ├── kubernetes/    # K8s manifests
│   └── terraform/     # Cloud infrastructure
│
├── notebooks/         # Jupyter notebooks
├── tests/             # Integration tests
├── scripts/           # Build and deployment scripts
└── docs/              # Documentation
```

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (20.10+) with 16GB RAM
- Python 3.11+
- Make (optional)
- 50GB free disk space

### Installation

```bash
# 1. Clone repository
git clone <repository-url>
cd nord-security-pipeline

# 2. Install all packages and services
make install

# 3. Start Docker services
make start

# 4. Check status
make status
```

### Makefile Commands

```bash
# Setup & Start
make install           # Install all packages/services
make start             # Start Docker services
make stop              # Stop Docker services
make status            # Check service status

# Development
make test              # Run all tests
make lint              # Lint all code
make format            # Format all code

# Service-specific
make install-ingestion # Install ingestion service
make test-ingestion    # Test ingestion service
```

## 📊 Data Flow

```
HTTP Sources → Ingestion Service → Kafka →
Spark Streaming (Bronze → Silver → Gold) →
Delta Lake → Trino (Query Engine)

Total Latency: ~60 seconds
```

## 🔧 Services

### Ingestion Service (`services/ingestion/`)
- Fetches data from HTTP sources
- Parses multiple formats (hosts, adblock)
- Validates and enriches data
- Publishes to Kafka

### Streaming Service (`services/streaming/`)
- Bronze Stream: Kafka → Bronze Delta
- Silver Stream: Clean & validate
- Gold Stream: Aggregate & optimize

### Orchestration Service (`services/orchestration/`)
- Airflow DAGs for scheduling
- Monitoring workflows

## 📦 Packages

### Common (`packages/common/`)
- Shared utilities
- Structured logging
- Custom exceptions

### Schemas (`packages/schemas/`)
- Data models
- Kafka message schemas
- Delta table schemas

### Monitoring (`packages/monitoring/`)
- Prometheus metrics
- Health checks
- Alerting

## 📖 Documentation

- [Architecture](docs/architecture/overview.md)
- [Setup Guide](docs/operations/setup.md)
- [Development Guide](docs/development/contributing.md)

See [docs/task_def/nord_readme_full.md](docs/task_def/nord_readme_full.md) for complete documentation.

## 🧪 Testing

```bash
# Run all tests
make test

# Run integration tests
make test-integration

# Run service-specific tests
make test-ingestion
make test-streaming
```

## 📝 License

MIT License - See LICENSE file for details

---

**Last Updated:** October 2025
**Version:** 1.0.0
**Architecture:** Monorepo
