.PHONY: help install test lint format clean setup start stop

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Installation
install: ## Install all packages and services
	@echo "Installing packages..."
	cd packages/common && poetry config virtualenvs.in-project true --local && poetry install
	cd packages/schemas && poetry config virtualenvs.in-project true --local && poetry install
	cd packages/monitoring && poetry config virtualenvs.in-project true --local && poetry install
	@echo "Installing services..."
	cd services/ingestion && poetry config virtualenvs.in-project true --local && poetry install
	cd services/streaming && poetry config virtualenvs.in-project true --local && poetry install

install-dev: ## Install all with development dependencies
	@echo "Installing packages in dev mode..."
	cd packages/common && poetry install
	cd packages/schemas && poetry install
	cd packages/monitoring && poetry install
	cd services/ingestion && poetry install
	cd services/streaming && poetry install

# Testing
test: ## Run tests for all services and packages
	@echo "Testing packages..."
	cd packages/common && poetry run pytest || [ $$? -eq 5 ]
	cd packages/schemas && poetry run pytest || [ $$? -eq 5 ]
	cd packages/monitoring && poetry run pytest || [ $$? -eq 5 ]
	@echo "Testing services..."
	cd services/ingestion && poetry run pytest || [ $$? -eq 5 ]
	cd services/streaming && poetry run pytest || [ $$? -eq 5 ]

test-integration: ## Run integration tests
	@echo "Running integration tests..."
	cd tests/integration && pytest -v

# Code Quality
lint: ## Run linting for all code
	@echo "Linting packages..."
	cd packages/common && poetry run ruff check src/
	cd packages/schemas && poetry run ruff check src/
	cd packages/monitoring && poetry run ruff check src/
	@echo "Linting services..."
	cd services/ingestion && poetry run ruff check src/
	cd services/streaming && poetry run ruff check src/

format: ## Format all code
	@echo "Formatting packages..."
	cd packages/common && poetry run black src/ tests/
	cd packages/schemas && poetry run black src/ tests/
	cd packages/monitoring && poetry run black src/ tests/
	@echo "Formatting services..."
	cd services/ingestion && poetry run black src/ tests/
	cd services/streaming && poetry run black src/ tests/

# Cleanup
clean: ## Clean build artifacts
	@echo "Cleaning build artifacts..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

clean-venv: ## Remove all virtual environments
	@echo "Removing all virtual environments..."
	find . -type d -name ".venv" -exec rm -rf {} + 2>/dev/null || true

clean-data: ## Clean data directories (checkpoints, Delta tables)
	@echo "Cleaning data directories..."
	rm -rf data/
	rm -rf checkpoints/
	rm -rf spark-warehouse/
	rm -rf metastore_db/
	rm -f derby.log

# Docker
start: ## Start all services with Docker Compose
	@echo "Starting services..."
	cd infrastructure/docker && docker-compose up -d

stop: ## Stop all services
	@echo "Stopping services..."
	cd infrastructure/docker && docker-compose down

restart: ## Restart all services
	@make stop
	@make start

status: ## Check service status
	@cd infrastructure/docker && docker-compose ps

logs: ## Show logs from all services
	@cd infrastructure/docker && docker-compose logs -f

# Docker Build
build-ingestion: ## Build ingestion service image
	@cd services/ingestion && docker build -t thread-protection-ingestion .

build-streaming: ## Build streaming service image
	@cd services/streaming && docker build -t thread-protection-streaming .

build-all: build-ingestion build-streaming ## Build all service images

# Service-specific targets
install-ingestion: ## Install ingestion service
	cd services/ingestion && poetry config virtualenvs.in-project true --local && poetry install

install-streaming: ## Install streaming service
	cd services/streaming && poetry config virtualenvs.in-project true --local && poetry install

test-ingestion: ## Test ingestion service
	cd services/ingestion && poetry run pytest

test-streaming: ## Test streaming service
	cd services/streaming && poetry run pytest

# Package-specific targets
install-common: ## Install common package
	cd packages/common && poetry config virtualenvs.in-project true --local && poetry install

install-schemas: ## Install schemas package
	cd packages/schemas && poetry config virtualenvs.in-project true --local && poetry install

install-monitoring: ## Install monitoring package
	cd packages/monitoring && poetry config virtualenvs.in-project true --local && poetry install
