# FILE: services/usage-api/tests/test_api.py
# Unit tests for Usage API Service

import pytest
from unittest.mock import Mock, MagicMock, patch
from main import app, UsageAPI
import base64

@pytest.fixture
def client():
    """Create test client"""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

@pytest.fixture
def auth_headers():
    """Create basic auth headers"""
    credentials = base64.b64encode(b"api_user:api_password").decode('utf-8')
    return {'Authorization': f'Basic {credentials}'}

def test_health_endpoint(client):
    """Test health check endpoint"""
    response = client.get('/health')
    assert response.status_code == 200
    assert b'healthy' in response.data

def test_data_usage_without_auth(client):
    """Test API requires authentication"""
    response = client.get('/data_usage?msisdn=2712345678&start_time=20240101000000&end_time=20240101235959')
    assert response.status_code == 401

def test_data_usage_with_auth(client, auth_headers):
    """Test authenticated request"""
    with patch('main.api') as mock_api:
        mock_api.get_usage.return_value = (
            {
                'msisdn': '2712345678',
                'start_time': '2024-01-01 00:00:00',
                'end_time': '2024-01-01 23:59:59',
                'usage': []
            },
            None
        )
        
        response = client.get(
            '/data_usage?msisdn=2712345678&start_time=20240101000000&end_time=20240101235959',
            headers=auth_headers
        )
        assert response.status_code == 200

def test_data_usage_missing_params(client, auth_headers):
    """Test API with missing parameters"""
    response = client.get('/data_usage', headers=auth_headers)
    assert response.status_code == 400

def test_data_usage_invalid_timestamp(client, auth_headers):
    """Test API with invalid timestamp format"""
    with patch('main.api') as mock_api:
        mock_api.get_usage.return_value = (
            None,
            "Invalid timestamp format. Use YYYYMMDDHHmmss"
        )
        
        response = client.get(
            '/data_usage?msisdn=2712345678&start_time=invalid&end_time=invalid',
            headers=auth_headers
        )
        assert response.status_code == 400

def test_usage_api_query():
    """Test UsageAPI query method"""
    with patch('main.Cluster') as mock_cluster:
        mock_session = Mock()
        mock_cluster.return_value.connect.return_value = mock_session
        
        # Mock query results
        mock_row = Mock()
        mock_row.date = '2024-01-01'
        mock_row.hour = 0
        mock_row.data_type = 'video'
        mock_row.up_bytes = 1000000
        mock_row.down_bytes = 2000000
        mock_row.call_duration_sec = 0
        mock_row.call_count = 0
        mock_row.updated_at = '2024-01-01 00:00:00'
        
        mock_session.execute.return_value = [mock_row]
        
        api = UsageAPI(['localhost'])
        result, error = api.get_usage('2712345678', '20240101000000', '20240101235959')
        
        assert error is None
        assert result['msisdn'] == '2712345678'
        assert len(result['usage']) > 0

def test_response_format(client, auth_headers):
    """Test response format matches specification"""
    with patch('main.api') as mock_api:
        mock_api.get_usage.return_value = (
            {
                'msisdn': '2712345678',
                'start_time': '2024-01-01 00:00:00',
                'end_time': '2024-01-01 23:59:59',
                'usage': [
                    {
                        'category': 'data',
                        'usage_type': 'video',
                        'total': 3000000,
                        'measure': 'bytes',
                        'start_time': '2024-01-01 00:00:00'
                    }
                ]
            },
            None
        )
        
        response = client.get(
            '/data_usage?msisdn=2712345678&start_time=20240101000000&end_time=20240101235959',
            headers=auth_headers
        )
        
        data = response.get_json()
        assert 'msisdn' in data
        assert 'start_time' in data
        assert 'end_time' in data
        assert 'usage' in data
        assert isinstance(data['usage'], list)