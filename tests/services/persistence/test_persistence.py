# FILE: services/persistence/tests/test_persistence.py
# Unit tests for Persistence Service

import pytest
from unittest.mock import Mock, MagicMock, patch
from main import PersistenceService
from datetime import datetime

@pytest.fixture
def db_url():
    return "postgresql://postgres:postgres@localhost:15432/wtc_analytics"

@pytest.fixture
def kafka_brokers():
    return ['localhost:19092']

@pytest.fixture
def service(db_url, kafka_brokers):
    with patch('main.create_engine') as mock_engine:
        mock_engine.return_value.connect.return_value.__enter__ = Mock()
        mock_engine.return_value.connect.return_value.__exit__ = Mock()
        return PersistenceService(db_url, kafka_brokers)

def test_service_initialization(db_url, kafka_brokers):
    """Test persistence service initialization"""
    with patch('main.create_engine') as mock_engine:
        service = PersistenceService(db_url, kafka_brokers)
        mock_engine.assert_called_once_with(db_url)

def test_table_creation(service):
    """Test tables are created"""
    assert service.cdr_data_table is not None
    assert service.cdr_voice_table is not None
    assert service.forex_table is not None
    assert service.crm_accounts_table is not None

def test_persist_cdr_data(service):
    """Test CDR data persistence"""
    record = {
        'msisdn': '2712345678',
        'tower_id': 1,
        'up_bytes': 1000000,
        'down_bytes': 2000000,
        'data_type': 'video',
        'ip_address': '1.2.3.4',
        'website_url': 'example.com',
        'event_datetime': '2024-01-01 12:00:00.000000'
    }
    
    with patch.object(service.engine, 'connect') as mock_connect:
        mock_conn = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        service.persist_cdr_data(record)
        
        assert mock_conn.execute.called
        assert mock_conn.commit.called

def test_persist_cdr_voice(service):
    """Test CDR voice persistence"""
    record = {
        'msisdn': '2712345678',
        'tower_id': 1,
        'call_type': 'voice',
        'dest_nr': '2798765432',
        'call_duration_sec': 120,
        'start_time': '2024-01-01 12:00:00.000000'
    }
    
    with patch.object(service.engine, 'connect') as mock_connect:
        mock_conn = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        service.persist_cdr_voice(record)
        
        assert mock_conn.execute.called
        assert mock_conn.commit.called

def test_persist_forex(service):
    """Test forex data persistence"""
    record = {
        'timestamp': '2024-01-01 12:00:00.000000',
        'pair_name': 'MRVZAR',
        'bid_price': 1.5000,
        'ask_price': 1.5050,
        'spread': 0.0050
    }
    
    with patch.object(service.engine, 'connect') as mock_connect:
        mock_conn = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        service.persist_forex(record)
        
        assert mock_conn.execute.called
        assert mock_conn.commit.called

def test_persist_crm_account(service):
    """Test CRM account persistence with upsert"""
    record = {
        'account_id': 12345,
        'owner_name': 'John Doe',
        'email': 'john@example.com',
        'phone_number': '2712345678',
        'modified_ts': datetime.now()
    }
    
    with patch.object(service.engine, 'connect') as mock_connect:
        mock_conn = Mock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        service.persist_crm_account(record)
        
        assert mock_conn.execute.called
        assert mock_conn.commit.called

def test_error_handling(service):
    """Test error handling during persistence"""
    record = {
        'msisdn': '2712345678',
        'tower_id': 1,
        'up_bytes': 'invalid',  # Should cause an error
        'down_bytes': 2000000,
        'data_type': 'video',
        'ip_address': '1.2.3.4',
        'website_url': 'example.com',
        'event_datetime': '2024-01-01 12:00:00.000000'
    }
    
    with patch.object(service.engine, 'connect') as mock_connect:
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Database error")
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Should not raise exception, just log error
        try:
            service.persist_cdr_data(record)
        except:
            pytest.fail("Exception should be caught and logged")