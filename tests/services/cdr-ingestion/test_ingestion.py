# FILE: services/cdr-ingestion/tests/test_ingestion.py
# Unit tests for CDR Ingestion Service

import pytest
from unittest.mock import Mock, MagicMock, patch
from main import CDRIngestion
import json

@pytest.fixture
def sftp_config():
    return {
        'host': 'localhost',
        'port': 10022,
        'username': 'cdr_data',
        'password': 'password'
    }

@pytest.fixture
def kafka_config():
    return {
        'brokers': ['localhost:19092']
    }

@pytest.fixture
def cdr_ingestion(sftp_config, kafka_config):
    with patch('main.KafkaProducer'):
        return CDRIngestion(sftp_config, kafka_config)

def test_producer_creation(sftp_config, kafka_config):
    """Test that Kafka producer is created correctly"""
    with patch('main.KafkaProducer') as mock_producer:
        ingestion = CDRIngestion(sftp_config, kafka_config)
        mock_producer.assert_called_once()

def test_sftp_connection(cdr_ingestion):
    """Test SFTP connection establishment"""
    with patch('paramiko.SSHClient') as mock_ssh:
        mock_sftp = Mock()
        mock_ssh.return_value.open_sftp.return_value = mock_sftp
        
        sftp = cdr_ingestion.connect_sftp()
        assert sftp == mock_sftp

def test_process_cdr_file(cdr_ingestion):
    """Test processing of CDR file"""
    mock_sftp = Mock()
    mock_file = Mock()
    csv_data = "msisdn,tower_id,up_bytes,down_bytes,data_type,ip_address,website_url,event_datetime\n"
    csv_data += "2712345678,1,1000,2000,video,1.2.3.4,example.com,2024-01-01 00:00:00.000000\n"
    mock_file.read.return_value = csv_data.encode('utf-8')
    mock_sftp.file.return_value.__enter__.return_value = mock_file
    
    cdr_ingestion.producer = Mock()
    
    count = cdr_ingestion.process_cdr_file(mock_sftp, 'test.csv', 'test-topic')
    
    assert count == 1
    assert cdr_ingestion.producer.send.called

def test_csv_parsing(cdr_ingestion):
    """Test CSV data parsing"""
    mock_sftp = Mock()
    mock_file = Mock()
    csv_data = "msisdn,tower_id\n2712345678,1\n2798765432,2\n"
    mock_file.read.return_value = csv_data.encode('utf-8')
    mock_sftp.file.return_value.__enter__.return_value = mock_file
    
    cdr_ingestion.producer = Mock()
    
    count = cdr_ingestion.process_cdr_file(mock_sftp, 'test.csv', 'test-topic')
    
    assert count == 2
    assert cdr_ingestion.producer.send.call_count == 2

def test_error_handling(cdr_ingestion):
    """Test error handling when file processing fails"""
    mock_sftp = Mock()
    mock_sftp.file.side_effect = Exception("SFTP error")
    
    count = cdr_ingestion.process_cdr_file(mock_sftp, 'test.csv', 'test-topic')
    
    assert count == 0

def test_producer_flush(cdr_ingestion):
    """Test that producer flushes after processing"""
    mock_sftp = Mock()
    mock_file = Mock()
    mock_file.read.return_value = "msisdn\n2712345678\n".encode('utf-8')
    mock_sftp.file.return_value.__enter__.return_value = mock_file
    
    cdr_ingestion.producer = Mock()
    
    cdr_ingestion.process_cdr_file(mock_sftp, 'test.csv', 'test-topic')
    
    assert cdr_ingestion.producer.flush.called