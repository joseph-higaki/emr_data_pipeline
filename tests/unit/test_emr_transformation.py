"""
Unit tests for the EMR transformation script.
"""
import os
import pytest
from unittest.mock import MagicMock, patch, mock_open
import pandas as pd
import tempfile
from datetime import datetime
from google.cloud import storage
import io

from scripts.emr_transformation.emr_transformation import (
    get_expected_entities,
    validate_timestamp,
    read_processed_timestamps,
    write_processed_timestamp,
    list_blobs,
    organize_files_by_timestamp,
    list_input_files,
    read_csv_from_gcs,
    extract_entity_name,
    upload_parquet_to_gcs,
    filter_timestamps_to_process
)

class TestEmrTransformation:
    """Tests for EMR transformation script functions."""
    
    def test_get_expected_entities_empty(self, monkeypatch):
        """Test get_expected_entities with empty environment variable."""
        monkeypatch.delenv('EMR_EXPECTED_ENTITIES', raising=False)
        entities = get_expected_entities()
        assert entities == set()
    
    def test_get_expected_entities(self, monkeypatch):
        """Test get_expected_entities with valid environment variable."""
        monkeypatch.setenv('EMR_EXPECTED_ENTITIES', 'patients,encounters,observations')
        entities = get_expected_entities()
        assert entities == {'patients', 'encounters', 'observations'}
    
    @pytest.mark.parametrize("timestamp,expected", [
        ('2023-01-01T00:00:00Z', True),
        ('2023-01-01T12:34:56Z', True),
        ('2023-01-01T24:00:00Z', False),  # Invalid hour
        ('2023-01-01T00:00:00', False),  # Missing Z
        ('2023-01-01 00:00:00Z', False),  # Wrong format
        ('invalid', False),
    ])
    def test_validate_timestamp(self, timestamp, expected):
        """Test validate_timestamp with various inputs."""
        assert validate_timestamp(timestamp) == expected
    
    @patch('pandas.read_csv')
    def test_read_processed_timestamps(self, mock_read_csv):
        """Test read_processed_timestamps."""
        # Setup mock
        mock_df = pd.DataFrame({
            'ingested_at': ['2023-01-01T00:00:00Z', '2023-01-02T00:00:00Z', 'invalid'],
            'transformed_at': ['2023-01-01T12:00:00Z', '2023-01-02T12:00:00Z', 'invalid']
        })
        mock_read_csv.return_value = mock_df
        
        # Call function
        result = read_processed_timestamps('test-bucket', 'processed/emr')
        
        # Assert results
        assert len(result) == 2
        assert result['2023-01-01T00:00:00Z'] == '2023-01-01T12:00:00Z'
        assert result['2023-01-02T00:00:00Z'] == '2023-01-02T12:00:00Z'
        assert 'invalid' not in result
    
    @patch('pandas.DataFrame.to_csv')
    @patch('scripts.emr_transformation.emr_transformation.datetime')
    def test_write_processed_timestamp(self, mock_datetime, mock_to_csv):
        """Test write_processed_timestamp."""
        # Setup mocks
        mock_now = MagicMock()
        mock_now.strftime.return_value = '2023-01-01T12:00:00Z'
        mock_datetime.now.return_value = mock_now
        
        # Setup test data
        existing_timestamps = {
            '2022-12-01T00:00:00Z': '2022-12-01T12:00:00Z'
        }
        
        # Call function
        result = write_processed_timestamp(
            'test-bucket', 'processed/emr', '2023-01-01T00:00:00Z', existing_timestamps
        )
        
        # Assert results
        assert len(result) == 2
        assert result['2022-12-01T00:00:00Z'] == '2022-12-01T12:00:00Z'
        assert result['2023-01-01T00:00:00Z'] == '2023-01-01T12:00:00Z'
        mock_to_csv.assert_called_once()
    
    def test_list_blobs(self):
        """Test list_blobs."""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_blobs.return_value = ['blob1', 'blob2']
        
        # Call function
        result = list(list_blobs('test-bucket', 'raw/emr', mock_client))
        
        # Assert results
        assert result == ['blob1', 'blob2']
        mock_client.list_blobs.assert_called_once_with('test-bucket', prefix='raw/emr')
    
    def test_organize_files_by_timestamp(self):
        """Test organize_files_by_timestamp."""
        # Setup mock blobs with proper name property
        mock_blobs = [
            MagicMock(spec=['name']),
            MagicMock(spec=['name']),
            MagicMock(spec=['name']),
            MagicMock(spec=['name']),
        ]
        
        # Configure the name attribute of each mock
        mock_blobs[0].name = 'raw/emr/2023-01-01T00:00:00Z/patients.csv'
        mock_blobs[1].name = 'raw/emr/2023-01-01T00:00:00Z/encounters.csv'
        mock_blobs[2].name = 'raw/emr/2023-01-02T00:00:00Z/patients.csv'
        mock_blobs[3].name = 'raw/emr/invalid/patients.csv'
        
        # Call function
        result = organize_files_by_timestamp(mock_blobs, 'raw/emr')
        
        # Assert results
        assert len(result) == 2
        assert len(result['2023-01-01T00:00:00Z']) == 2
        assert len(result['2023-01-02T00:00:00Z']) == 1
        assert 'invalid' not in result
    
    @patch('scripts.emr_transformation.emr_transformation.list_blobs')
    @patch('scripts.emr_transformation.emr_transformation.organize_files_by_timestamp')
    def test_list_input_files(self, mock_organize, mock_list_blobs):
        """Test list_input_files."""
        # Setup mocks
        mock_list_blobs.return_value = ['blob1', 'blob2']
        mock_organize.return_value = {
            '2023-01-01T00:00:00Z': ['raw/emr/2023-01-01T00:00:00Z/patients.csv'],
            '2023-01-02T00:00:00Z': ['raw/emr/2023-01-02T00:00:00Z/encounters.csv'],
        }
        
        # Call function
        result = list_input_files('test-bucket', 'raw/emr', MagicMock())
        
        # Assert results
        assert len(result) == 2
        assert result['2023-01-01T00:00:00Z'] == ['raw/emr/2023-01-01T00:00:00Z/patients.csv']
        assert result['2023-01-02T00:00:00Z'] == ['raw/emr/2023-01-02T00:00:00Z/encounters.csv']
    
    @patch('pandas.read_csv')
    def test_read_csv_from_gcs(self, mock_read_csv):
        """Test read_csv_from_gcs."""
        # Setup mock
        mock_df = pd.DataFrame({'id': ['1', '2'], 'name': ['John', 'Jane']})
        mock_read_csv.return_value = mock_df
        
        # Call function
        result = read_csv_from_gcs('gs://test-bucket/raw/emr/patients.csv')
        
        # Assert results
        assert result.equals(mock_df)
        mock_read_csv.assert_called_once()
    
    def test_extract_entity_name(self):
        """Test extract_entity_name."""
        assert extract_entity_name('/path/to/Patients.csv') == 'patients'
        assert extract_entity_name('Encounters.csv') == 'encounters'
        assert extract_entity_name('/tmp/observations.CSV') == 'observations'
    
    @patch('tempfile.NamedTemporaryFile')    
    @patch('pyarrow.parquet.write_table')
    @patch('scripts.emr_transformation.emr_transformation.pa', new_callable=MagicMock)  # py arrow
    def test_upload_parquet_to_gcs(self, mock_pyarrow, mock_write, mock_temp_file):
        """Test upload_parquet_to_gcs."""
        # Setup mocks
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        mock_temp_file_obj = MagicMock()
        mock_temp_file_obj.name = '/tmp/test.parquet'
        mock_temp_file_obj.__enter__.return_value = mock_temp_file_obj
        mock_temp_file.return_value = mock_temp_file_obj
        
        # Setup test data
        df = pd.DataFrame({'id': ['1', '2'], 'name': ['John', 'Jane']})
        
        # Call function
        with patch('uuid.uuid4', return_value=MagicMock(hex='12345')):
            result = upload_parquet_to_gcs(
                df, 'patients', '2023-01-01T00:00:00Z', 
                'test-bucket', 'processed/emr', mock_client
            )
        
        # Assert results
        assert result == 'gs://test-bucket/processed/emr/patients/ingested_at=2023-01-01T00:00:00Z/patients_12345.parquet'
        mock_client.bucket.assert_called_once_with('test-bucket')
        mock_pyarrow.Table.from_pandas.assert_called_once()
        mock_write.assert_called_once()
        mock_blob.upload_from_filename.assert_called_once_with('/tmp/test.parquet')
    
    def test_filter_timestamps_to_process_no_processed(self):
        """Test filter_timestamps_to_process with no processed timestamps."""
        # Setup test data
        timestamps = [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
        ]
        processed_timestamps = {}
        
        # Call function
        result = filter_timestamps_to_process(timestamps, processed_timestamps)
        
        # Assert results
        assert result == timestamps
    
    def test_filter_timestamps_to_process_with_processed(self):
        """Test filter_timestamps_to_process with processed timestamps."""
        # Setup test data
        timestamps = [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
            datetime(2023, 1, 3),
        ]
        processed_timestamps = {
            '2023-01-01T00:00:00Z': '2023-01-01T12:00:00Z',
            '2023-01-02T00:00:00Z': '2023-01-02T12:00:00Z',
        }
        
        # Call function
        result = filter_timestamps_to_process(timestamps, processed_timestamps)
        
        # Assert results
        assert len(result) == 1
        assert result[0] == datetime(2023, 1, 3)
