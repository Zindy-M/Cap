#!/bin/bash
# FILE: deploy.sh
# Main deployment script for the entire project

set -e

echo "=========================================="
echo "WTC Data Engineering Project Deployment"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker Desktop."
    exit 1
fi

print_status "Docker is running"

# Clean up old containers and volumes (optional)
if [ "$1" == "--clean" ]; then
    print_warning "Cleaning up old containers and volumes..."
    docker compose down -v
    rm -rf volumes/data/*
fi

# Create necessary directories
print_status "Creating directories..."
mkdir -p volumes/data/sftp
mkdir -p volumes/data/postgres
mkdir -p volumes/data/scylla
mkdir -p volumes/config

# Build all custom images
print_status "Building custom Docker images..."
docker compose build

# Start infrastructure services
print_status "Starting infrastructure services..."
docker compose up -d postgres sftp redpanda-0 redpanda-1 redpanda-2 zookeeper

# Wait for Postgres to be ready
print_status "Waiting for PostgreSQL to be ready..."
until docker exec postgres pg_isready -U postgres > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo ""
print_status "PostgreSQL is ready"

# Wait for Redpanda to be ready
print_status "Waiting for Redpanda cluster to be ready..."
sleep 30

# Start Redpanda Console
print_status "Starting Redpanda Console..."
docker compose up -d redpanda-console

# Start Debezium
print_status "Starting Debezium..."
docker compose up -d debezium

# Wait for Debezium to be ready
print_status "Waiting for Debezium to be ready..."
until curl -s http://localhost:8083/ > /dev/null 2>&1; do
    echo -n "."
    sleep 5
done
echo ""
print_status "Debezium is ready"

# Setup Debezium connector
print_status "Configuring Debezium CDC connector..."
python3 scripts/setup-debezium.py

# Start ScyllaDB
print_status "Starting ScyllaDB..."
docker compose up -d scylla

# Wait for ScyllaDB to be ready
print_status "Waiting for ScyllaDB to be ready (this may take 1-2 minutes)..."
sleep 60

# Initialize ScyllaDB schema
print_status "Initializing ScyllaDB schema..."
python3 scripts/init-scylla.py

# Start data generator services
print_status "Starting data generators (CDR, CRM, Forex)..."
docker compose up -d cdr crm forex

# Wait a bit for data to start generating
sleep 10

# Start processing services
print_status "Starting CDR ingestion service..."
docker compose up -d cdr-ingestion

print_status "Starting stream processor..."
docker compose up -d stream-processor

print_status "Starting ScyllaDB consumer..."
docker compose up -d scylla-consumer

print_status "Starting persistence service..."
docker compose up -d persistence

# Wait for services to initialize
sleep 15

# Start Usage API
print_status "Starting Usage API..."
docker compose up -d usage-api

# Wait for API to be ready
print_status "Waiting for Usage API to be ready..."
until curl -s http://localhost:18089/health > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo ""
print_status "Usage API is ready"

# Run DBT models (if dbt is set up)
if [ -d "dbt" ]; then
    print_status "Running DBT models..."
    cd dbt
    if command -v dbt &> /dev/null; then
        dbt run
        cd ..
    else
        print_warning "DBT not installed. Skipping DBT models."
        cd ..
    fi
fi

# Print status
echo ""
echo "=========================================="
print_status "Deployment Complete!"
echo "=========================================="
echo ""
echo "Services running:"
echo "  - PostgreSQL:        localhost:15432"
echo "  - SFTP:              localhost:10022"
echo "  - Redpanda Console:  http://localhost:18084"
echo "  - Debezium:          http://localhost:8083"
echo "  - ScyllaDB:          localhost:9042"
echo "  - Usage API:         http://localhost:18089"
echo ""
echo "Data generators:"
echo "  - CDR:    $(docker ps --filter name=cdr --format '{{.Status}}')"
echo "  - CRM:    $(docker ps --filter name=crm --format '{{.Status}}')"
echo "  - Forex:  $(docker ps --filter name=forex --format '{{.Status}}')"
echo ""
echo "Processing services:"
echo "  - CDR Ingestion:     $(docker ps --filter name=cdr-ingestion --format '{{.Status}}')"
echo "  - Stream Processor:  $(docker ps --filter name=stream-processor --format '{{.Status}}')"
echo "  - ScyllaDB Consumer: $(docker ps --filter name=scylla-consumer --format '{{.Status}}')"
echo "  - Persistence:       $(docker ps --filter name=persistence --format '{{.Status}}')"
echo ""
echo "Test the API:"
echo "  curl -u api_user:api_password \"http://localhost:18089/data_usage?msisdn=2712345678&start_time=20240101000000&end_time=20240101235959\""
echo ""
echo "View logs:"
echo "  docker compose logs -f [service_name]"
echo ""
echo "Stop all services:"
echo "  docker compose down"
echo "=========================================="