# FILE: services/persistence/main.py
# Persistence Service - Stores all Kafka data to PostgreSQL

from kafka import KafkaConsumer
from sqlalchemy import create_engine, Table, Column, Integer, String, BigInteger, Float, DateTime, MetaData
from sqlalchemy.dialects.postgresql import insert
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PersistenceService:
    def __init__(self, db_url, kafka_brokers):
        self.engine = create_engine(db_url)
        self.metadata = MetaData()
        self.kafka_brokers = kafka_brokers
        self.setup_tables()
        
    def setup_tables(self):
        """Create tables in PostgreSQL"""
        
        # CDR Data table
        self.cdr_data_table = Table(
            'cdr_data', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('msisdn', String(20)),
            Column('tower_id', Integer),
            Column('up_bytes', BigInteger),
            Column('down_bytes', BigInteger),
            Column('data_type', String(50)),
            Column('ip_address', String(45)),
            Column('website_url', String(500)),
            Column('event_datetime', DateTime),
            Column('ingested_at', DateTime, default=datetime.now),
            schema='cdr_data'
        )
        
        # CDR Voice table
        self.cdr_voice_table = Table(
            'cdr_voice', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('msisdn', String(20)),
            Column('tower_id', Integer),
            Column('call_type', String(50)),
            Column('dest_nr', String(20)),
            Column('call_duration_sec', Integer),
            Column('start_time', DateTime),
            Column('ingested_at', DateTime, default=datetime.now),
            schema='cdr_data'
        )
        
        # Forex tick data table
        self.forex_table = Table(
            'forex_ticks', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('timestamp', DateTime),
            Column('pair_name', String(10)),
            Column('bid_price', Float),
            Column('ask_price', Float),
            Column('spread', Float),
            Column('ingested_at', DateTime, default=datetime.now),
            schema='forex_data'
        )
        
        # CRM tables (from CDC)
        self.crm_accounts_table = Table(
            'accounts', self.metadata,
            Column('account_id', Integer, primary_key=True),
            Column('owner_name', String(100)),
            Column('email', String(100)),
            Column('phone_number', String(100)),
            Column('modified_ts', DateTime),
            Column('ingested_at', DateTime, default=datetime.now),
            schema='crm_data'
        )
        
        self.crm_addresses_table = Table(
            'addresses', self.metadata,
            Column('account_id', Integer, primary_key=True),
            Column('street_address', String(100)),
            Column('city', String(100)),
            Column('state', String(100)),
            Column('postal_code', String(100)),
            Column('country', String(100)),
            Column('modified_ts', DateTime),
            Column('ingested_at', DateTime, default=datetime.now),
            schema='crm_data'
        )
        
        self.crm_devices_table = Table(
            'devices', self.metadata,
            Column('device_id', Integer, primary_key=True),
            Column('account_id', Integer),
            Column('device_name', String(100)),
            Column('device_type', String(100)),
            Column('device_os', String(100)),
            Column('modified_ts', DateTime),
            Column('ingested_at', DateTime, default=datetime.now),
            schema='crm_data'
        )
        
        # Create all tables
        self.metadata.create_all(self.engine)
        logger.info("Tables created successfully")
    
    def persist_cdr_data(self, record):
        """Persist CDR data record to PostgreSQL"""
        with self.engine.connect() as conn:
            stmt = insert(self.cdr_data_table).values(
                msisdn=record['msisdn'],
                tower_id=record['tower_id'],
                up_bytes=record['up_bytes'],
                down_bytes=record['down_bytes'],
                data_type=record['data_type'],
                ip_address=record['ip_address'],
                website_url=record['website_url'],
                event_datetime=datetime.strptime(record['event_datetime'], "%Y-%m-%d %H:%M:%S.%f")
            )
            conn.execute(stmt)
            conn.commit()
    
    def persist_cdr_voice(self, record):
        """Persist CDR voice record to PostgreSQL"""
        with self.engine.connect() as conn:
            stmt = insert(self.cdr_voice_table).values(
                msisdn=record['msisdn'],
                tower_id=record['tower_id'],
                call_type=record['call_type'],
                dest_nr=record['dest_nr'],
                call_duration_sec=record['call_duration_sec'],
                start_time=datetime.strptime(record['start_time'], "%Y-%m-%d %H:%M:%S.%f")
            )
            conn.execute(stmt)
            conn.commit()
    
    def persist_forex(self, record):
        """Persist forex tick data to PostgreSQL"""
        with self.engine.connect() as conn:
            stmt = insert(self.forex_table).values(
                timestamp=datetime.strptime(record['timestamp'], "%Y-%m-%d %H:%M:%S.%f"),
                pair_name=record['pair_name'],
                bid_price=record['bid_price'],
                ask_price=record['ask_price'],
                spread=record['spread']
            )
            conn.execute(stmt)
            conn.commit()
    
    def persist_crm_account(self, record):
        """Persist CRM account from CDC"""
        with self.engine.connect() as conn:
            stmt = insert(self.crm_accounts_table).values(
                account_id=record['account_id'],
                owner_name=record['owner_name'],
                email=record['email'],
                phone_number=record['phone_number'],
                modified_ts=record['modified_ts']
            ).on_conflict_do_update(
                index_elements=['account_id'],
                set_=dict(
                    owner_name=record['owner_name'],
                    email=record['email'],
                    phone_number=record['phone_number'],
                    modified_ts=record['modified_ts']
                )
            )
            conn.execute(stmt)
            conn.commit()
    
    def run(self):
        """Main consumer loop"""
        consumer = KafkaConsumer(
            'cdr-data',
            'cdr-voice',
            'tick-data',
            'crm.crm_system.accounts',
            'crm.crm_system.addresses',
            'crm.crm_system.devices',
            bootstrap_servers=self.kafka_brokers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='persistence-service',
            auto_offset_reset='earliest'
        )
        
        logger.info("Starting persistence service...")
        record_count = 0
        
        try:
            for message in consumer:
                topic = message.topic
                record = message.value
                
                try:
                    if topic == 'cdr-data':
                        self.persist_cdr_data(record)
                    elif topic == 'cdr-voice':
                        self.persist_cdr_voice(record)
                    elif topic == 'tick-data':
                        self.persist_forex(record)
                    elif 'accounts' in topic:
                        if 'after' in record:
                            self.persist_crm_account(record['after'])
                    
                    record_count += 1
                    if record_count % 1000 == 0:
                        logger.info(f"Persisted {record_count} records")
                        
                except Exception as e:
                    logger.error(f"Error persisting record: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            consumer.close()

if __name__ == "__main__":
    db_url = "postgresql://postgres:postgres@localhost:15432/wtc_analytics"
    kafka_brokers = ['localhost:19092', 'localhost:29092', 'localhost:39092']
    
    service = PersistenceService(db_url, kafka_brokers)
    service.run()