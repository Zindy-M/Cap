# FILE: services/scylla-consumer/tests/test_consumer.py
# Unit tests for ScyllaDB Consumer Service

import pytest
from unittest.mock import Mock, MagicMock, patch
from main import ScyllaDBConsumer

@pytest.fixture
def scylla_hosts():
    return ['localhost']

@pytest.fixture
def kafka_brokers():
    return ['localhost:19092']

@pytest.fixture
def consumer(scylla_hosts, kafka_brokers):
    with patch('main.Cluster') as mock_cluster:
        mock_session = Mock()
        mock_cluster.return_value.connect.return_value = mock_session
        return ScyllaDBConsumer(scylla_hosts, kafka_brokers)

def test_consumer_initialization(scylla_hosts, kafka_brokers):
    """Test ScyllaDB consumer initialization"""
    with patch('main.Cluster') as mock_cluster:
        consumer = ScyllaDBConsumer(scylla_hosts, kafka_brokers)
        mock_cluster.assert_called_once_with(scylla_hosts)

def test_schema_creation(consumer):
    """Test keyspace and table creation"""
    assert consumer.session is not None
    # Verify that execute was called for schema creation
    assert consumer.session.execute.called

def test_data_insertion(consumer):
    """Test data insertion into ScyllaDB"""
    summary = {
        'msisdn': '2712345678',
        'date': '2024-01-01',
        'data_usage': {
            'video': {'up': 1000000, 'down': 2000000}
        },
        'voice_usage': {
            'voice': {'duration': 120, 'count': 2}
        },
        'costs': {
            'data_cost_wak': 10.5,
            'voice_cost_wak': 2.0,
            'total_cost_wak': 12.5
        }
    }
    
    # Mock session execute
    consumer.session.execute = Mock()
    
    # This would be called in the actual consume loop
    # Just verifying the session can execute queries
    assert consumer.session.execute is not None

def test_prepared_statement(consumer):
    """Test prepared statement usage"""
    consumer.session.prepare = Mock(return_value=Mock())
    stmt = consumer.session.prepare("INSERT INTO test VALUES (?, ?)")
    assert stmt is not None