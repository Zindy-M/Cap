# FILE: Makefile
# Makefile for common project tasks

.PHONY: help build up down clean test logs restart deploy

help:
	@echo "Data Engineering Project - Available Commands"
	@echo "=============================================="
	@echo "make build     - Build all Docker images"
	@echo "make up        - Start all services"
	@echo "make down      - Stop all services"
	@echo "make clean     - Stop services and remove volumes"
	@echo "make test      - Run all tests"
	@echo "make logs      - Show logs from all services"
	@echo "make restart   - Restart all services"
	@echo "make deploy    - Full deployment with setup"
	@echo "make dbt-run   - Run DBT models"
	@echo "make dbt-test  - Run DBT tests"

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

clean:
	docker compose down -v
	rm -rf volumes/data/*

test:
	./test-all.sh

logs:
	docker compose logs -f

restart:
	docker compose restart

deploy:
	./deploy.sh

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

# Service-specific commands
logs-cdr-ingestion:
	docker compose logs -f cdr-ingestion

logs-stream-processor:
	docker compose logs -f stream-processor

logs-api:
	docker compose logs -f usage-api

# Database commands
psql-prod:
	docker exec -it postgres psql -U postgres -d wtc_prod

psql-analytics:
	docker exec -it postgres psql -U postgres -d wtc_analytics

cqlsh:
	docker exec -it scylla cqlsh

# Kafka/Redpanda commands
kafka-topics:
	docker exec redpanda-0 rpk topic list

kafka-consume-cdr:
	docker exec redpanda-0 rpk topic consume cdr-data

# Health checks
health:
	@echo "Checking service health..."
	@curl -s http://localhost:18089/health || echo "API not responding"
	@docker exec postgres pg_isready -U postgres || echo "PostgreSQL not ready"
	@docker exec redpanda-0 rpk cluster health || echo "Redpanda not healthy"
	@docker exec scylla cqlsh -e "describe cluster" || echo "ScyllaDB not ready"