import pytest
import json
from unittest.mock import MagicMock, patch
from airflow.models import Connection, DagBag

@pytest.fixture
def mock_postgres_hook():
    """Mock PostgreSQL hook for testing"""
    mock_hook = MagicMock(spec=PostgresHook)
    mock_hook.get_records.return_value = [(1,)]  # Simulating returned ID
    return mock_hook

@pytest.fixture
def sample_phishing_data():
    """Load sample phishing data for testing"""
    with open('tests/data/sample_response.json', 'r') as f:
        return json.load(f)

@pytest.fixture
def mock_requests_get():
    """Mock requests.get for API testing"""
    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        with open('tests/data/sample_response.json', 'r') as f:
            mock_response.json.return_value = json.load(f)
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        yield mock_get

@pytest.fixture
def mock_context():
    """Mock Airflow context for testing"""
    context = {
        'task_instance': MagicMock()
    }
    return context

@pytest.fixture
def dag_bag():
    """Load DAG for testing"""
    return DagBag(dag_folder='dags', include_examples=False) 