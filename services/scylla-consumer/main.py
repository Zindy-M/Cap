# FILE: services/scylla-consumer/main.py
# ScyllaDB Consumer - Stores usage data in ScyllaDB

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from kafka import KafkaConsumer
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScyllaDBConsumer:
    def __init__(self, scylla_hosts, kafka_brokers):
        self.cluster = Cluster(scylla_hosts)
        self.session = None
        self.kafka_brokers = kafka_brokers
        self.setup_schema()
        
    def setup_schema(self):
        """Create keyspace and tables in ScyllaDB"""
        self.session = self.cluster.connect()
        
        # Create keyspace
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS cdr_usage
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        
        self.session.set_keyspace('cdr_usage')
        
        # Create table for daily usage summaries
        self.session.execute("""
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
        
        logger.info("ScyllaDB schema created successfully")
    
    def consume_and_store(self):
        """Consume usage summaries from Kafka and store in ScyllaDB"""
        consumer = KafkaConsumer(
            'cdr-usage-summary',
            bootstrap_servers=self.kafka_brokers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='scylla-consumer',
            auto_offset_reset='earliest'
        )
        
        prepare_stmt = self.session.prepare("""
            INSERT INTO daily_usage (
                msisdn, date, hour, data_type, 
                up_bytes, down_bytes, call_duration_sec, call_count,
                data_cost_wak, voice_cost_wak, total_cost_wak, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        logger.info("Starting to consume usage summaries...")
        
        try:
            for message in consumer:
                summary = message.value
                msisdn = summary['msisdn']
                date = summary['date']
                
                # Store data usage
                for data_type, stats in summary.get('data_usage', {}).items():
                    self.session.execute(prepare_stmt, (
                        msisdn,
                        date,
                        0,  # hour (we're doing daily aggregation)
                        data_type,
                        stats['up'],
                        stats['down'],
                        0,  # call_duration_sec
                        0,  # call_count
                        summary['costs']['data_cost_wak'],
                        0,
                        summary['costs']['total_cost_wak'],
                        datetime.now()
                    ))
                
                # Store voice usage
                for call_type, stats in summary.get('voice_usage', {}).items():
                    self.session.execute(prepare_stmt, (
                        msisdn,
                        date,
                        0,
                        call_type,
                        0,  # up_bytes
                        0,  # down_bytes
                        stats['duration'],
                        stats['count'],
                        0,
                        summary['costs']['voice_cost_wak'],
                        summary['costs']['total_cost_wak'],
                        datetime.now()
                    ))
                
                logger.info(f"Stored usage for {msisdn} on {date}")
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            consumer.close()
            self.cluster.shutdown()

if __name__ == "__main__":
    scylla_hosts = ['localhost']
    kafka_brokers = ['localhost:19092', 'localhost:29092', 'localhost:39092']
    
    consumer = ScyllaDBConsumer(scylla_hosts, kafka_brokers)
    consumer.consume_and_store()