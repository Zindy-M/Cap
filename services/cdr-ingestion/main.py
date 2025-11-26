# FILE: services/cdr-ingestion/main.py
# CDR Ingestion Service - Downloads CDR files from SFTP and streams to Redpanda

import paramiko
import csv
from kafka import KafkaProducer
import json
import logging
import time
from io import StringIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CDRIngestion:
    def __init__(self, sftp_config, kafka_config):
        self.sftp_config = sftp_config
        self.kafka_config = kafka_config
        self.producer = self._create_producer()
        
    def _create_producer(self):
        """Create Kafka producer for Redpanda"""
        return KafkaProducer(
            bootstrap_servers=self.kafka_config['brokers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None
        )
    
    def connect_sftp(self):
        """Connect to SFTP server"""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=self.sftp_config['host'],
            port=self.sftp_config['port'],
            username=self.sftp_config['username'],
            password=self.sftp_config['password']
        )
        return ssh.open_sftp()
    
    def process_cdr_file(self, sftp, filename, topic):
        """Download and process a CDR file"""
        try:
            # Download file
            with sftp.file(filename, 'r') as remote_file:
                content = remote_file.read().decode('utf-8')
            
            # Parse CSV
            csv_reader = csv.DictReader(StringIO(content))
            
            # Stream to Redpanda
            count = 0
            for row in csv_reader:
                key = row.get('msisdn', '')
                self.producer.send(topic, key=key, value=row)
                count += 1
                
                if count % 1000 == 0:
                    logger.info(f"Processed {count} records from {filename}")
            
            self.producer.flush()
            logger.info(f"Completed {filename}: {count} records")
            return count
            
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            return 0
    
    def run(self):
        """Main ingestion loop"""
        sftp = self.connect_sftp()
        processed_files = set()
        
        while True:
            try:
                # List files
                files = sftp.listdir('.')
                
                for filename in files:
                    if filename in processed_files:
                        continue
                    
                    if filename.startswith('cdr_data_'):
                        self.process_cdr_file(sftp, filename, 'cdr-data')
                        processed_files.add(filename)
                    elif filename.startswith('cdr_voice_'):
                        self.process_cdr_file(sftp, filename, 'cdr-voice')
                        processed_files.add(filename)
                
                time.sleep(10)  # Check for new files every 10 seconds
                
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(30)
        
        sftp.close()
        self.producer.close()

if __name__ == "__main__":
    sftp_config = {
        'host': 'localhost',
        'port': 10022,
        'username': 'cdr_data',
        'password': 'password'
    }
    
    kafka_config = {
        'brokers': ['localhost:19092', 'localhost:29092', 'localhost:39092']
    }
    
    ingestion = CDRIngestion(sftp_config, kafka_config)
    ingestion.run()