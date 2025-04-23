"""
Unit tests for the EMR ingestion script.
"""
import os
import pytest
from unittest.mock import MagicMock, patch, mock_open
import tempfile
import zipfile
import io
from datetime import datetime
import shutil

from scripts.emr_ingestion.emr_ingestion import (
    get_expected_entities,
    extract_entity_name,
    download_zip_file,
    extract_zip_file,
    read_emr_patient_data,
    upload_to_gcs,
    process_files
)

class TestEmrIngestion:
    """Tests for EMR ingestion script functions."""
    
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
    
    def test_extract_entity_name(self):
        """Test extract_entity_name with different paths."""
        assert extract_entity_name('/path/to/Patients.csv') == 'patients'
        assert extract_entity_name('Encounters.csv') == 'encounters'
        assert extract_entity_name('observations.csv') == 'observations'
    
    @patch('requests.get')
    def test_download_zip_file(self, mock_get):
        """Test download_zip_file."""
        mock_response = MagicMock()
        mock_response.content = b'zip_content'
        mock_get.return_value = mock_response
        
        content = download_zip_file('http://example.com/data.zip')
        assert content == b'zip_content'
        mock_get.assert_called_once_with('http://example.com/data.zip')
    
    def test_extract_zip_file(self):
        """Test extract_zip_file."""
        # Create a test ZIP file
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_content = io.BytesIO()
            with zipfile.ZipFile(zip_content, 'w') as zipf:
                zipf.writestr('patients.csv', 'id,name\n1,John')
                zipf.writestr('encounters.csv', 'id,patient_id\n1,1')
            
            # Test extraction
            file_list = extract_zip_file(zip_content.getvalue(), temp_dir)
            assert set(file_list) == {'patients.csv', 'encounters.csv'}
            
            # Verify files were extracted
            assert os.path.exists(os.path.join(temp_dir, 'patients.csv'))
            assert os.path.exists(os.path.join(temp_dir, 'encounters.csv'))
    
    @patch('scripts.emr_ingestion.emr_ingestion.download_zip_file')
    @patch('scripts.emr_ingestion.emr_ingestion.extract_zip_file')
    def test_read_emr_patient_data(self, mock_extract, mock_download):
        """Test read_emr_patient_data."""
        # Setup mocks
        mock_download.return_value = b'zip_content'
        mock_extract.return_value = ['patients.csv', 'encounters.csv']
        
        # Create temporary directory and files for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            with open(os.path.join(temp_dir, 'patients.csv'), 'w') as f:
                f.write('id,name\n1,John')
            with open(os.path.join(temp_dir, 'encounters.csv'), 'w') as f:
                f.write('id,patient_id\n1,1')
            
            # Patch tempfile.mkdtemp to return our temp_dir
            with patch('tempfile.mkdtemp', return_value=temp_dir):
                # Call the function
                result = list(read_emr_patient_data('http://example.com/data.zip'))
                
                # Assert results
                assert len(result) == 2
                paths, names = zip(*result)
                assert set(names) == {'patients.csv', 'encounters.csv'}
                assert all(os.path.exists(path) for path in paths)
    
    def test_upload_to_gcs(self):
        """Test upload_to_gcs."""
        # Create mock storage client and bucket
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Create a temporary file for testing
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(b'test content')
            temp_file.flush()
            
            # Call the function
            result = upload_to_gcs(
                '2023-01-01T00:00:00Z',
                temp_file.name,
                'patients.csv',
                'test-bucket',
                'raw/emr',
                mock_client
            )
            
            # Assert results
            assert result == 'gs://test-bucket/raw/emr/2023-01-01T00:00:00Z/patients.csv'
            mock_client.bucket.assert_called_once_with('test-bucket')
            mock_bucket.blob.assert_called_once_with('raw/emr/2023-01-01T00:00:00Z/patients.csv')
            mock_blob.upload_from_filename.assert_called_once_with(temp_file.name)
    
    @patch('scripts.emr_ingestion.emr_ingestion.read_emr_patient_data')
    @patch('scripts.emr_ingestion.emr_ingestion.upload_to_gcs')
    @patch('datetime.datetime')
    def test_process_files(self, mock_datetime, mock_upload, mock_read_data):
        """Test process_files."""
        # Setup mocks
        mock_now = MagicMock()
        mock_now.strftime.return_value = '2023-01-01T00:00:00Z'
        mock_datetime.now.return_value = mock_now
        
        # Setup file data
        mock_read_data.return_value = [
            ('/tmp/patients.csv', 'patients.csv'),
            ('/tmp/encounters.csv', 'encounters.csv'),
            ('/tmp/unknown.csv', 'unknown.csv'),
        ]
        
        mock_upload.side_effect = [
            'gs://test-bucket/raw/emr/2023-01-01T00:00:00Z/patients.csv',
            'gs://test-bucket/raw/emr/2023-01-01T00:00:00Z/encounters.csv',
        ]
        
        # Call the function
        expected_entities = {'patients', 'encounters'}
        uploaded, skipped = process_files(
            'http://example.com/data.zip',
            'test-bucket',
            'raw/emr',
            expected_entities,
            MagicMock()
        )
        
        # Assert results
        assert len(uploaded) == 2
        assert len(skipped) == 1
        assert uploaded == [
            'gs://test-bucket/raw/emr/2023-01-01T00:00:00Z/patients.csv',
            'gs://test-bucket/raw/emr/2023-01-01T00:00:00Z/encounters.csv',
        ]
        assert skipped == ['unknown.csv']

@pytest.mark.parametrize("file_path,expected", [
    ('/path/to/Patients.csv', 'patients'),
    ('Encounters.csv', 'encounters'),
    ('/tmp/observations.CSV', 'observations'),
])
def test_extract_entity_name_parametrized(file_path, expected):
    """Parametrized test for extract_entity_name."""
    assert extract_entity_name(file_path) == expected
