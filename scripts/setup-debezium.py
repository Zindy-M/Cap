# FILE: scripts/setup-debezium.py
# Script to configure Debezium CDC connector for PostgreSQL

import requests
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_debezium_connector():
    """Create Debezium connector for PostgreSQL CDC"""
    
    connector_config = {
        "name": "crm-postgres-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname": "wtc_prod",
            "database.server.name": "wtc_crm",
            "table.include.list": "crm_system.accounts,crm_system.addresses,crm_system.devices",
            "plugin.name": "pgoutput",
            "publication.autocreate.mode": "filtered",
            "topic.prefix": "crm",
            "schema.history.internal.kafka.bootstrap.servers": "redpanda-0:9092",
            "schema.history.internal.kafka.topic": "schema-changes.crm"
        }
    }
    
    # Wait for Debezium to be ready
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get("http://localhost:8083/")
            if response.status_code == 200:
                logger.info("Debezium is ready")
                break
        except:
            logger.info(f"Waiting for Debezium... ({i+1}/{max_retries})")
            time.sleep(10)
    
    # Create connector
    try:
        response = requests.post(
            "http://localhost:8083/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config)
        )
        
        if response.status_code in [200, 201]:
            logger.info("Connector created successfully")
            logger.info(response.json())
        else:
            logger.error(f"Failed to create connector: {response.status_code}")
            logger.error(response.text)
            
    except Exception as e:
        logger.error(f"Error creating connector: {e}")

if __name__ == "__main__":
    create_debezium_connector()