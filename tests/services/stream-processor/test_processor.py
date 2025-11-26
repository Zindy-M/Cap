# FILE: services/stream-processor/tests/test_processor.py
# Unit tests for Stream Processor Service

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from main import CDRStreamProcessor
import json

@pytest.fixture
def kafka_brokers():
    return ['localhost:19092']

@pytest.fixture
def processor(kafka_brokers):
    with patch('main.KafkaConsumer'), patch('main.KafkaProducer'):
        return CDRStreamProcessor(kafka_brokers)

def test_processor_initialization(kafka_brokers):
    """Test processor initializes correctly"""
    with patch('main.KafkaConsumer') as mock_consumer, \
         patch('main.KafkaProducer') as mock_producer:
        processor = CDRStreamProcessor(kafka_brokers)
        mock_consumer.assert_called_once()
        mock_producer.assert_called_once()
        assert len(processor.daily_usage) == 0

def test_get_day_key(processor):
    """Test date extraction from timestamp"""
    timestamp = "2024-01-15 14:30:45.123456"
    day_key = processor.get_day_key(timestamp)
    assert day_key == "2024-01-15"

def test_process_data_record(processor):
    """Test processing of data CDR record"""
    record = {
        'msisdn': '2712345678',
        'event_datetime': '2024-01-01 12:00:00.000000',
        'data_type': 'video',
        'up_bytes': 1000000,
        'down_bytes': 2000000
    }
    
    processor.process_data_record(record)
    
    key = "2712345678:2024-01-01"
    assert key in processor.daily_usage
    assert processor.daily_usage[key]['data']['video']['up'] == 1000000
    assert processor.daily_usage[key]['data']['video']['down'] == 2000000

def test_process_voice_record(processor):
    """Test processing of voice CDR record"""
    record = {
        'msisdn': '2712345678',
        'start_time': '2024-01-01 12:00:00.000000',
        'call_type': 'voice',
        'call_duration_sec': 120
    }
    
    processor.process_voice_record(record)
    
    key = "2712345678:2024-01-01"
    assert key in processor.daily_usage
    assert processor.daily_usage[key]['voice']['voice']['duration'] == 120
    assert processor.daily_usage[key]['voice']['voice']['count'] == 1

def test_cost_calculation(processor):
    """Test cost calculation accuracy"""
    # Data cost: 1GB = 49 ZAR
    bytes_in_gb = 1024 * 1024 * 1024
    data_cost = bytes_in_gb * processor.DATA_RATE_PER_BYTE
    assert abs(data_cost - 49.0) < 0.01
    
    # Voice cost: 60 seconds = 1 ZAR
    voice_cost = 60 * processor.VOICE_RATE_PER_SEC
    assert abs(voice_cost - 1.0) < 0.01

def test_multiple_records_aggregation(processor):
    """Test aggregation of multiple records"""
    records = [
        {
            'msisdn': '2712345678',
            'event_datetime': '2024-01-01 12:00:00.000000',
            'data_type': 'video',
            'up_bytes': 1000000,
            'down_bytes': 2000000
        },
        {
            'msisdn': '2712345678',
            'event_datetime': '2024-01-01 13:00:00.000000',
            'data_type': 'video',
            'up_bytes': 500000,
            'down_bytes': 1000000
        }
    ]
    
    for record in records:
        processor.process_data_record(record)
    
    key = "2712345678:2024-01-01"
    assert processor.daily_usage[key]['data']['video']['up'] == 1500000
    assert processor.daily_usage[key]['data']['video']['down'] == 3000000

def test_flush_usage(processor):
    """Test usage data flush"""
    processor.producer = Mock()
    
    # Add some usage data
    key = "2712345678:2024-01-01"
    processor.daily_usage[key] = {
        'data': {'video': {'up': 1000000, 'down': 2000000}},
        'voice': {'voice': {'duration': 120, 'count': 2}},
        'last_update': datetime.now()
    }
    
    processor.flush_usage(key, '2712345678', '2024-01-01')
    
    # Verify producer was called
    assert processor.producer.send.called
    
    # Verify data was removed from memory
    assert key not in processor.daily_usage

def test_different_data_types(processor):
    """Test handling of different data types"""
    data_types = ['video', 'audio', 'image', 'text', 'application']
    
    for i, data_type in enumerate(data_types):
        record = {
            'msisdn': '2712345678',
            'event_datetime': '2024-01-01 12:00:00.000000',
            'data_type': data_type,
            'up_bytes': 1000 * (i + 1),
            'down_bytes': 2000 * (i + 1)
        }
        processor.process_data_record(record)
    
    key = "2712345678:2024-01-01"
    assert len(processor.daily_usage[key]['data']) == 5