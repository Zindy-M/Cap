#!/bin/bash
# FILE: test-all.sh
# Script to run all unit and integration tests

set -e

echo "=========================================="
echo "Running All Tests"
echo "=========================================="

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

FAILED_TESTS=0

# Function to run tests for a service
run_service_tests() {
    local service=$1
    echo ""
    echo "Testing $service..."
    
    if [ -d "services/$service/tests" ]; then
        cd "services/$service"
        
        # Create virtual environment if it doesn't exist
        if [ ! -d "venv" ]; then
            python3 -m venv venv
        fi
        
        # Activate virtual environment
        source venv/bin/activate
        
        # Install dependencies
        pip install -q -r requirements.txt
        
        # Run tests
        if pytest tests/ -v --cov=. --cov-report=term-missing; then
            echo -e "${GREEN}✓ $service tests passed${NC}"
        else
            echo -e "${RED}✗ $service tests failed${NC}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
        
        # Deactivate virtual environment
        deactivate
        
        cd ../..
    else
        echo "No tests found for $service"
    fi
}

# Test all services
run_service_tests "cdr-ingestion"
run_service_tests "stream-processor"
run_service_tests "scylla-consumer"
run_service_tests "usage-api"
run_service_tests "persistence"

# Integration tests
echo ""
echo "Running integration tests..."

# Test API endpoint
echo "Testing Usage API..."
API_RESPONSE=$(curl -s -u api_user:api_password "http://localhost:18089/health")
if [[ $API_RESPONSE == *"healthy"* ]]; then
    echo -e "${GREEN}✓ API health check passed${NC}"
else
    echo -e "${RED}✗ API health check failed${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi

# Test Redpanda connection
echo "Testing Redpanda..."
if docker exec redpanda-0 rpk cluster health > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Redpanda cluster healthy${NC}"
else
    echo -e "${RED}✗ Redpanda cluster unhealthy${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi

# Test PostgreSQL connection
echo "Testing PostgreSQL..."
if docker exec postgres pg_isready -U postgres > /dev/null 2>&1; then
    echo -e "${GREEN}✓ PostgreSQL connection successful${NC}"
else
    echo -e "${RED}✗ PostgreSQL connection failed${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi

# Test ScyllaDB connection
echo "Testing ScyllaDB..."
if docker exec scylla cqlsh -e "describe cluster" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ ScyllaDB connection successful${NC}"
else
    echo -e "${RED}✗ ScyllaDB connection failed${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi

# DBT tests
if [ -d "dbt" ]; then
    echo ""
    echo "Running DBT tests..."
    cd dbt
    if command -v dbt &> /dev/null; then
        if dbt test; then
            echo -e "${GREEN}✓ DBT tests passed${NC}"
        else
            echo -e "${RED}✗ DBT tests failed${NC}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    else
        echo "DBT not installed, skipping"
    fi
    cd ..
fi

# Summary
echo ""
echo "=========================================="
if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}$FAILED_TESTS test(s) failed${NC}"
    exit 1
fi