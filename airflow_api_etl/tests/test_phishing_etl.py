import pytest
from datetime import datetime
import json
from unittest.mock import patch, MagicMock
import sys
import os

# Add the parent directory of 'dag_files' to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dag_files.phishing_data_etl import (
    fetch_phishing_data,
    transform_phishing_data,
    load_to_database
)

def test_dag_loaded(dag_bag):
    """Test that our DAG is loaded correctly"""
    dag = dag_bag.get_dag(dag_id='phishing_data_etl')
    assert dag is not None
    assert dag.dag_id == 'phishing_data_etl'
    assert len(dag.tasks) == 4  # create_tables, fetch, transform, load

def test_fetch_phishing_data(mock_requests_get, mock_context, sample_phishing_data):
    """Test the fetch_phishing_data function"""
    # Execute the function
    fetch_phishing_data(**mock_context)
    
    # Verify API was called correctly
    mock_requests_get.assert_called_once_with("https://phishstats.info:2096/api/phishing")
    
    # Verify data was stored in XCom
    mock_context['task_instance'].xcom_push.assert_called_once()
    args = mock_context['task_instance'].xcom_push.call_args[1]
    assert args['key'] == 'raw_phishing_data'
    assert args['value'] == sample_phishing_data

def test_transform_phishing_data(mock_context, sample_phishing_data):
    """Test the transform_phishing_data function"""
    # Setup mock to return our sample data
    mock_context['task_instance'].xcom_pull.return_value = sample_phishing_data
    
    # Execute the function
    transform_phishing_data(**mock_context)
    
    # Verify transformation
    mock_context['task_instance'].xcom_push.assert_called_once()
    args = mock_context['task_instance'].xcom_push.call_args[1]
    assert args['key'] == 'transformed_data'
    
    transformed_data = args['value']
    assert len(transformed_data) == len(sample_phishing_data)
    
    # Check first record's transformation
    record = transformed_data[0]
    assert 'main' in record
    assert 'metadata' in record
    assert 'tags' in record
    
    # Verify geohash was generated
    assert record['main']['geohash'] is not None
    
    # Verify main record fields
    main_data = record['main']
    assert main_data['external_id'] == sample_phishing_data[0]['id']
    assert main_data['url'] == sample_phishing_data[0]['url']
    assert main_data['hash'] == sample_phishing_data[0]['hash']
    
    # Verify metadata fields
    metadata = record['metadata']
    assert metadata['screenshot_url'] == sample_phishing_data[0]['screenshot']
    assert metadata['abuse_contact'] == sample_phishing_data[0]['abuse_contact']
    
    # Verify tags
    assert len(record['tags']) == len(sample_phishing_data[0]['tags'])
    assert all(tag['hash'] == sample_phishing_data[0]['hash'] for tag in record['tags'])

@patch('airflow.providers.postgres.hooks.postgres.PostgresHook')
def test_load_to_database(mock_hook_class, mock_context, sample_phishing_data):
    """Test the load_to_database function"""
    # Setup mock
    mock_hook = mock_hook_class.return_value
    mock_hook.get_records.return_value = [(1,)]  # Simulate returned ID
    
    # Create transformed data
    mock_context['task_instance'].xcom_pull.return_value = [{
        'main': {
            'external_id': 2,
            'url': 'http://test-phish.com',
            'ip': '192.168.1.1',
            'hash': 'abc123def456',
            'host': 'test-phish.com'
        },
        'metadata': {
            'screenshot_url': 'http://test.com/screen.png'
        },
        'tags': [{'hash': 'abc123def456', 'tag': 'phishing'}]
    }]
    
    # Execute the function
    load_to_database(**mock_context)
    
    # Verify database calls
    assert mock_hook.get_records.call_count == 1  # Main record insert
    assert mock_hook.run.call_count == 2  # Metadata and tag inserts

def test_fetch_phishing_data_error_handling(mock_context):
    """Test error handling in fetch_phishing_data"""
    with patch('requests.get') as mock_get:
        mock_get.side_effect = Exception("API Error")
        
        with pytest.raises(Exception) as exc_info:
            fetch_phishing_data(**mock_context)
        
        assert "API Error" in str(exc_info.value)

def test_transform_phishing_data_error_handling(mock_context):
    """Test error handling in transform_phishing_data"""
    # Setup mock to return invalid data
    mock_context['task_instance'].xcom_pull.return_value = [{'invalid': 'data'}]
    
    # Function should handle the error and continue
    transform_phishing_data(**mock_context)
    
    # Verify empty transformed data was pushed
    mock_context['task_instance'].xcom_push.assert_called_once()
    args = mock_context['task_instance'].xcom_push.call_args[1]
    assert args['key'] == 'transformed_data'
    assert len(args['value']) == 0

def test_load_to_database_error_handling(mock_context, mock_postgres_hook):
    """Test error handling in load_to_database"""
    # Setup mock to raise an exception
    mock_postgres_hook.get_records.side_effect = Exception("Database Error")
    
    # Create test data
    mock_context['task_instance'].xcom_pull.return_value = [{
        'main': {'hash': 'test'},
        'metadata': {},
        'tags': []
    }]
    
    # Function should handle the error and continue
    load_to_database(**mock_context)
    
    # Verify attempt was made to insert
    assert mock_postgres_hook.get_records.call_count == 1 