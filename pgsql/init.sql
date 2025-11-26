-- FILE: pgsql/init.sql
-- PostgreSQL initialization script with CDC configuration

-- Initialize databases and schemas

-- Create production database
CREATE DATABASE wtc_prod;

-- Create analytics database
CREATE DATABASE wtc_analytics;

-- Create Airflow database (optional, for future use)
CREATE DATABASE airflow;

-- Connect to wtc_prod and set up CRM schemas
\c wtc_prod;

-- Create CRM schema
CREATE SCHEMA IF NOT EXISTS crm_system;

-- Create CRM tables
CREATE TABLE IF NOT EXISTS crm_system.accounts (
    account_id INTEGER PRIMARY KEY,
    owner_name VARCHAR(100),
    email VARCHAR(100),
    phone_number VARCHAR(100),
    modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crm_system.addresses (
    account_id INTEGER PRIMARY KEY,
    street_address VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(100),
    country VARCHAR(100),
    modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crm_system.devices (
    device_id INTEGER PRIMARY KEY,
    account_id INTEGER,
    device_name VARCHAR(100),
    device_type VARCHAR(100),
    device_os VARCHAR(100),
    modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES crm_system.accounts(account_id)
);

-- Create publication for Debezium CDC
-- This enables logical replication for the CRM tables
CREATE PUBLICATION debezium_publication FOR TABLE 
    crm_system.accounts,
    crm_system.addresses,
    crm_system.devices;

-- Connect to wtc_analytics and set up schemas
\c wtc_analytics;

-- Create schemas for analytics
CREATE SCHEMA IF NOT EXISTS cdr_data;
CREATE SCHEMA IF NOT EXISTS crm_data;
CREATE SCHEMA IF NOT EXISTS forex_data;
CREATE SCHEMA IF NOT EXISTS prepared_layers;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA cdr_data TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA crm_data TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA forex_data TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA prepared_layers TO postgres;