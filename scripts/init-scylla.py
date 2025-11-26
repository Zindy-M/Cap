# FILE: scripts/init-scylla.py
# Script to initialize ScyllaDB schema

from cassandra.cluster import Cluster
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_scylla():
    """Initialize ScyllaDB keyspace and tables"""
    
    # Wait for ScyllaDB to be ready
    max_retries = 30
    for i in range(max_retries):
        try:
            cluster = Cluster(['localhost'], port=9042)
            session = cluster.connect()
            logger.info("ScyllaDB is ready")
            break
        except Exception as e:
            logger.info(f"Waiting for ScyllaDB... ({i+1}/{max_retries})")
            time.sleep(10)
    else:
        logger.error("ScyllaDB failed to start")
        return
    
    # Create keyspace
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS cdr_usage
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        logger.info("Keyspace created successfully")
        
        session.set_keyspace('cdr_usage')
        
        # Create table for daily usage summaries
        session.execute("""
            CREATE TABLE IF NOT EXISTS daily_usage (
                msisdn text,
                date text,
                hour int,
                data_type text,
                up_bytes bigint,
                down_bytes bigint,
                call_duration_sec int,
                call_count int,
                data_cost_wak decimal,
                voice_cost_wak decimal,
                total_cost_wak decimal,
                updated_at timestamp,
                PRIMARY KEY ((msisdn, date), hour, data_type)
            ) WITH CLUSTERING ORDER BY (hour ASC, data_type ASC)
        """)
        logger.info("Tables created successfully")
        
    except Exception as e:
        logger.error(f"Error creating schema: {e}")
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    init_scylla()