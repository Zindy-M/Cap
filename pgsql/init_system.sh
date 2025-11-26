#!/bin/bash
# FILE: pgsql/init_system.sh (FIXED)
# PostgreSQL initialization script

set -e

# Create production database
psql -U postgres -d postgres -c "CREATE DATABASE wtc_prod;"

# Connect to wtc_prod and create CRM schema
psql -U postgres -d wtc_prod -c "CREATE SCHEMA crm_system;"

# Create CRM tables
psql -U postgres -d wtc_prod <<EOF
CREATE TABLE IF NOT EXISTS crm_system.accounts (
    account_id INTEGER PRIMARY KEY, 
    owner_name VARCHAR(100), 
    email VARCHAR(100), 
    phone_number VARCHAR(100), 
    modified_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crm_system.addresses (
    account_id INTEGER PRIMARY KEY, 
    street_address VARCHAR(100), 
    city VARCHAR(100), 
    state VARCHAR(100), 
    postal_code VARCHAR(100), 
    country VARCHAR(100), 
    modified_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crm_system.devices (
    device_id INTEGER PRIMARY KEY, 
    account_id INTEGER, 
    device_name VARCHAR(100), 
    device_type VARCHAR(100), 
    device_os VARCHAR(100), 
    modified_ts TIMESTAMP
);
EOF

# Create analytics database
psql -U postgres -d postgres -c "CREATE DATABASE wtc_analytics;"

# Create Airflow database
psql -U postgres -d postgres -c "CREATE DATABASE airflow;"

# Create analytics schemas
psql -U postgres -d wtc_analytics <<EOF
CREATE SCHEMA cdr_data;
CREATE SCHEMA crm_data;
CREATE SCHEMA forex_data;
CREATE SCHEMA prepared_layers;
EOF

# Create Airflow schema
psql -U postgres -d airflow -c "CREATE SCHEMA airflow;"

echo "Database initialization complete"