# FILE: services/stream-processor/main.py
# Stream Processor - Aggregates CDR data in real-time

from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict
from datetime import datetime, timedelta
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CDRStreamProcessor:
    def __init__(self, kafka_brokers):
        self.kafka_brokers = kafka_brokers
        self.consumer = self._create_consumer()
        self.producer = self._create_producer()
        
        # In-memory state for daily aggregations
        self.daily_usage = defaultdict(lambda: {
            'data': defaultdict(lambda: {'up': 0, 'down': 0}),
            'voice': defaultdict(lambda: {'duration': 0, 'count': 0}),
            'last_update': None
        })
        
        # Pricing constants
        self.VOICE_RATE_PER_SEC = 1.0 / 60  # 1 ZAR per minute
        self.DATA_RATE_PER_BYTE = 49.0 / (1024**3)  # 49 ZAR per GB
        
    def _create_consumer(self):
        return KafkaConsumer(
            'cdr-data',
            'cdr-voice',
            bootstrap_servers=self.kafka_brokers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='cdr-stream-processor',
            auto_offset_reset='earliest'
        )
    
    def _create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.kafka_brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def get_day_key(self, timestamp_str):
        """Extract date from timestamp for daily aggregation"""
        try:
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        except:
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d")
    
    def process_data_record(self, record):
        """Process CDR data record"""
        msisdn = record['msisdn']
        day_key = self.get_day_key(record['event_datetime'])
        data_type = record['data_type']
        
        key = f"{msisdn}:{day_key}"
        usage = self.daily_usage[key]
        
        usage['data'][data_type]['up'] += int(record['up_bytes'])
        usage['data'][data_type]['down'] += int(record['down_bytes'])
        usage['last_update'] = datetime.now()
        
        # Check if we should flush (every 1000 updates or 60 seconds)
        self.check_and_flush(key, msisdn, day_key)
    
    def process_voice_record(self, record):
        """Process CDR voice record"""
        msisdn = record['msisdn']
        day_key = self.get_day_key(record['start_time'])
        call_type = record['call_type']
        
        key = f"{msisdn}:{day_key}"
        usage = self.daily_usage[key]
        
        usage['voice'][call_type]['duration'] += int(record['call_duration_sec'])
        usage['voice'][call_type]['count'] += 1
        usage['last_update'] = datetime.now()
        
        self.check_and_flush(key, msisdn, day_key)
    
    def check_and_flush(self, key, msisdn, day_key):
        """Flush aggregated data to output topic"""
        usage = self.daily_usage[key]
        
        # Flush if last update was more than 60 seconds ago
        if usage['last_update']:
            elapsed = (datetime.now() - usage['last_update']).seconds
            if elapsed > 60:
                self.flush_usage(key, msisdn, day_key)
    
    def flush_usage(self, key, msisdn, day_key):
        """Send aggregated usage to output topic"""
        usage = self.daily_usage[key]
        
        # Calculate costs
        data_cost = 0
        for data_type, stats in usage['data'].items():
            total_bytes = stats['up'] + stats['down']
            data_cost += total_bytes * self.DATA_RATE_PER_BYTE
        
        voice_cost = 0
        for call_type, stats in usage['voice'].items():
            voice_cost += stats['duration'] * self.VOICE_RATE_PER_SEC
        
        summary = {
            'msisdn': msisdn,
            'date': day_key,
            'data_usage': dict(usage['data']),
            'voice_usage': dict(usage['voice']),
            'costs': {
                'data_cost_wak': round(data_cost, 2),
                'voice_cost_wak': round(voice_cost, 2),
                'total_cost_wak': round(data_cost + voice_cost, 2)
            },
            'timestamp': datetime.now().isoformat()
        }
        
        # Send to output topic for HVS ingestion
        self.producer.send('cdr-usage-summary', value=summary)
        logger.info(f"Flushed usage for {msisdn} on {day_key}")
        
        # Clear from memory
        del self.daily_usage[key]
    
    def run(self):
        """Main processing loop"""
        logger.info("Starting CDR stream processor...")
        
        try:
            for message in self.consumer:
                topic = message.topic
                record = message.value
                
                if topic == 'cdr-data':
                    self.process_data_record(record)
                elif topic == 'cdr-voice':
                    self.process_voice_record(record)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.producer.close()
            self.consumer.close()

if __name__ == "__main__":
    kafka_brokers = ['localhost:19092', 'localhost:29092', 'localhost:39092']
    processor = CDRStreamProcessor(kafka_brokers)
    processor.run()