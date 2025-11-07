# Makefile for Quantitative Trading System
.PHONY: help install install-dev clean test lint format run docker-build docker-up docker-down docker-logs docker-clean setup-db migrate backup restore deploy

# Variables
PYTHON := python3.11
VENV := venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
BLACK := $(VENV)/bin/black
RUFF := $(VENV)/bin/ruff
MYPY := $(VENV)/bin/mypy
DOCKER_COMPOSE := docker-compose
APP_NAME := quant-trading-system
VERSION := $(shell git describe --tags --always --dirty)

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(GREEN)Quantitative Trading System - Makefile Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

install: ## Install production dependencies
	@echo "$(GREEN)Setting up virtual environment...$(NC)"
	$(PYTHON) -m venv $(VENV)
	@echo "$(GREEN)Installing production dependencies...$(NC)"
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements.txt
	@echo "$(GREEN)Installation complete!$(NC)"

install-dev: install ## Install development dependencies
	@echo "$(GREEN)Installing development dependencies...$(NC)"
	$(PIP) install -r requirements-dev.txt
	@echo "$(GREEN)Setting up pre-commit hooks...$(NC)"
	$(VENV)/bin/pre-commit install
	@echo "$(GREEN)Development installation complete!$(NC)"

clean: ## Clean up generated files and caches
	@echo "$(GREEN)Cleaning up...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build dist htmlcov .coverage coverage.xml
	@echo "$(GREEN)Cleanup complete!$(NC)"

test: ## Run all tests with coverage
	@echo "$(GREEN)Running tests with coverage...$(NC)"
	$(PYTEST) tests/ \
		--cov=src \
		--cov-report=html \
		--cov-report=term-missing \
		--cov-report=xml \
		-v

test-unit: ## Run unit tests only
	@echo "$(GREEN)Running unit tests...$(NC)"
	$(PYTEST) tests/unit/ -v

test-integration: ## Run integration tests only
	@echo "$(GREEN)Running integration tests...$(NC)"
	$(PYTEST) tests/integration/ -v

test-performance: ## Run performance tests
	@echo "$(GREEN)Running performance tests...$(NC)"
	$(PYTEST) tests/performance/ -v --benchmark-only

lint: ## Run linting checks
	@echo "$(GREEN)Running linting checks...$(NC)"
	$(RUFF) check src tests
	$(MYPY) src --ignore-missing-imports
	$(BLACK) --check src tests
	@echo "$(GREEN)Linting complete!$(NC)"

format: ## Format code with black and isort
	@echo "$(GREEN)Formatting code...$(NC)"
	$(BLACK) src tests
	$(VENV)/bin/isort src tests
	$(RUFF) check --fix src tests
	@echo "$(GREEN)Formatting complete!$(NC)"

security: ## Run security checks
	@echo "$(GREEN)Running security checks...$(NC)"
	$(VENV)/bin/bandit -r src -ll
	$(VENV)/bin/safety check
	@echo "$(GREEN)Security checks complete!$(NC)"

run: ## Run the application locally
	@echo "$(GREEN)Starting application...$(NC)"
	$(VENV)/bin/uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

run-celery: ## Run Celery worker
	@echo "$(GREEN)Starting Celery worker...$(NC)"
	$(VENV)/bin/celery -A src.core.celery_app worker --loglevel=info

run-celery-beat: ## Run Celery beat scheduler
	@echo "$(GREEN)Starting Celery beat...$(NC)"
	$(VENV)/bin/celery -A src.core.celery_app beat --loglevel=info

docker-build: ## Build Docker images
	@echo "$(GREEN)Building Docker images...$(NC)"
	$(DOCKER_COMPOSE) build --no-cache

docker-up: ## Start all Docker services
	@echo "$(GREEN)Starting Docker services...$(NC)"
	$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)Services started! Check status with 'make docker-ps'$(NC)"

docker-down: ## Stop all Docker services
	@echo "$(GREEN)Stopping Docker services...$(NC)"
	$(DOCKER_COMPOSE) down

docker-restart: docker-down docker-up ## Restart all Docker services

docker-logs: ## Show Docker logs
	$(DOCKER_COMPOSE) logs -f

docker-ps: ## Show Docker container status
	@echo "$(GREEN)Docker container status:$(NC)"
	$(DOCKER_COMPOSE) ps

docker-clean: docker-down ## Clean Docker volumes and images
	@echo "$(RED)Warning: This will delete all data in Docker volumes!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		$(DOCKER_COMPOSE) down -v; \
		docker system prune -af; \
		echo "$(GREEN)Docker cleanup complete!$(NC)"; \
	fi

setup-db: ## Initialize database schema
	@echo "$(GREEN)Setting up database...$(NC)"
	$(PYTHON) scripts/setup_db.py
	@echo "$(GREEN)Database setup complete!$(NC)"

migrate: ## Run database migrations
	@echo "$(GREEN)Running database migrations...$(NC)"
	$(VENV)/bin/alembic upgrade head
	@echo "$(GREEN)Migrations complete!$(NC)"

backup: ## Backup database
	@echo "$(GREEN)Creating database backup...$(NC)"
	@mkdir -p backups
	docker exec quant-timescaledb pg_dump -U quant quant_trading | gzip > backups/backup_$(shell date +%Y%m%d_%H%M%S).sql.gz
	@echo "$(GREEN)Backup complete!$(NC)"

restore: ## Restore database from backup
	@echo "$(GREEN)Available backups:$(NC)"
	@ls -la backups/*.sql.gz
	@read -p "Enter backup filename to restore: " backup_file; \
	if [ -f "backups/$$backup_file" ]; then \
		gunzip -c "backups/$$backup_file" | docker exec -i quant-timescaledb psql -U quant quant_trading; \
		echo "$(GREEN)Restore complete!$(NC)"; \
	else \
		echo "$(RED)Backup file not found!$(NC)"; \
	fi

jupyter: ## Start Jupyter Lab
	@echo "$(GREEN)Starting Jupyter Lab...$(NC)"
	$(VENV)/bin/jupyter lab --ip=0.0.0.0 --port=8888 --no-browser

profile: ## Profile the application
	@echo "$(GREEN)Running profiler...$(NC)"
	$(VENV)/bin/py-spy record -o profile.svg -- $(PYTHON) src/main.py

docs: ## Generate documentation
	@echo "$(GREEN)Generating documentation...$(NC)"
	$(VENV)/bin/mkdocs build
	@echo "$(GREEN)Documentation generated in site/ directory$(NC)"

docs-serve: ## Serve documentation locally
	@echo "$(GREEN)Serving documentation at http://localhost:8001$(NC)"
	$(VENV)/bin/mkdocs serve

deploy-staging: ## Deploy to staging environment
	@echo "$(GREEN)Deploying to staging...$(NC)"
	./scripts/deploy.sh staging

deploy-production: ## Deploy to production environment
	@echo "$(RED)Deploying to PRODUCTION!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		./scripts/deploy.sh production; \
	fi

monitor: ## Open monitoring dashboards
	@echo "$(GREEN)Opening monitoring dashboards...$(NC)"
	@echo "Grafana: http://localhost:3000 (admin/admin123)"
	@echo "Prometheus: http://localhost:9090"
	@echo "Flower (Celery): http://localhost:5555"
	@open http://localhost:3000 2>/dev/null || xdg-open http://localhost:3000 2>/dev/null || echo "Please open manually"

version: ## Show version information
	@echo "$(GREEN)$(APP_NAME) version: $(VERSION)$(NC)"
	@echo "Python: $(shell $(PYTHON) --version)"
	@echo "Docker: $(shell docker --version)"
	@echo "Docker Compose: $(shell docker-compose --version)"

.DEFAULT_GOAL := help