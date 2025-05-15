#!/usr/bin/env python3
"""
Unit tests for the ORC File Information Collector
"""

import unittest
from unittest.mock import patch, MagicMock, mock_open
import io
import tempfile
import os
import sys
import pandas as pd

# Import the module
from orc_info_collector import (
    parse_s3_path, 
    get_orc_file_info, 
    S3ORCReader,
    process_file_batch
)


class TestORCInfoCollector(unittest.TestCase):
    
    def test_parse_s3_path(self):
        """Test parsing S3 paths into bucket and key components."""
        # Test valid S3 path
        bucket, key = parse_s3_path('s3://my-bucket/path/to/file.orc')
        self.assertEqual(bucket, 'my-bucket')
        self.assertEqual(key, 'path/to/file.orc')
        
        # Test invalid S3 path
        with self.assertRaises(ValueError):
            parse_s3_path('http://example.com/file.orc')
    
    @patch('pyarrow.fs.S3FileSystem')
    def test_s3_orc_reader_with_pyarrow_fs(self, mock_s3_fs):
        """Test the S3ORCReader class with PyArrow filesystem."""
        # Mock S3 client
        mock_s3 = MagicMock()
        
        # Mock PyArrow S3 filesystem
        mock_fs_instance = MagicMock()
        mock_s3_fs.return_value = mock_fs_instance
        
        # Mock file info
        mock_file_info = MagicMock()
        mock_file_info.size = 10240  # 10KB file
        mock_fs_instance.get_file_info.return_value = mock_file_info
        
        # Create a mock ORC file reader
        with patch('pyarrow.orc.ORCFile') as mock_orc_file:
            mock_orc_instance = MagicMock()
            mock_orc_instance.nstripes = 3
            
            # Mock metadata with content_length
            mock_metadata = MagicMock()
            mock_metadata.content_length = 5000
            mock_orc_instance.metadata = mock_metadata
            
            # Mock statistics as fallback
            mock_orc_instance.statistics = {
                'col1': {'numberOfValues': 100, 'bytesOnDisk': 2000},
                'col2': {'numberOfValues': 200, 'bytesOnDisk': 3000}
            }
            mock_orc_file.return_value = mock_orc_instance
            
            # Create the reader and get metadata
            reader = S3ORCReader(mock_s3)
            metadata = reader.get_orc_metadata('my-bucket', 'path/to/file.orc')
            
            # Verify the result
            self.assertEqual(metadata['file_length'], 10240)
            self.assertEqual(metadata['num_stripes'], 3)
            self.assertEqual(metadata['raw_data_size'], 5000)  # Should use content_length
            
            # Verify PyArrow filesystem calls
            mock_fs_instance.get_file_info.assert_called_once_with('s3://my-bucket/path/to/file.orc')
            mock_orc_file.assert_called_once_with('s3://my-bucket/path/to/file.orc', filesystem=mock_fs_instance)
    
    @patch('boto3.client')
    def test_s3_orc_reader_legacy(self, mock_boto3_client):
        """Test the S3ORCReader class with legacy method."""
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        
        # Mock head_object response
        mock_s3.head_object.return_value = {
            'ContentLength': 10240  # 10KB file
        }
        
        # Mock get_object response for the end of the file
        mock_end_body = MagicMock()
        # Create a mock postscript and footer
        mock_end_data = bytearray(8192)
        # Set the postscript size (last byte)
        mock_end_data[-1] = 20
        mock_end_body.read.return_value = bytes(mock_end_data)
        mock_s3.get_object.return_value = {
            'Body': mock_end_body
        }
        
        # Create a mock ORC file reader
        with patch('pyarrow.orc.ORCFile') as mock_orc_file:
            mock_orc_instance = MagicMock()
            mock_orc_instance.nstripes = 3
            mock_orc_instance.statistics = {
                'col1': {'numberOfValues': 100},
                'col2': {'numberOfValues': 200}
            }
            mock_orc_file.return_value = mock_orc_instance
            
            # Force legacy method by making PyArrow filesystem fail
            with patch('pyarrow.fs.S3FileSystem', side_effect=Exception("Forced failure")):
                # Create the reader and get metadata
                reader = S3ORCReader(mock_s3)
                metadata = reader._legacy_get_metadata('my-bucket', 'path/to/file.orc')
                
                # Verify the result
                self.assertEqual(metadata['file_length'], 10240)
                self.assertEqual(metadata['num_stripes'], 3)
                
                # Verify S3 client calls
                mock_s3.head_object.assert_called_once_with(Bucket='my-bucket', Key='path/to/file.orc')
                mock_s3.get_object.assert_called_once()
    
    @patch('orc_info_collector.S3ORCReader')
    def test_get_orc_file_info(self, mock_s3_orc_reader):
        """Test getting ORC file information."""
        # Mock S3ORCReader
        mock_reader_instance = MagicMock()
        mock_reader_instance.get_orc_metadata.return_value = {
            'file_length': 1024,
            'num_stripes': 3,
            'raw_data_size': 300
        }
        mock_s3_orc_reader.return_value = mock_reader_instance
        
        # Call the function
        result = get_orc_file_info('s3://my-bucket/path/to/file.orc')
        
        # Verify the result
        self.assertEqual(result['file_path'], 's3://my-bucket/path/to/file.orc')
        self.assertEqual(result['file_length'], 1024)
        self.assertEqual(result['num_stripes'], 3)
        self.assertEqual(result['raw_data_size'], 300)
    
    @patch('orc_info_collector.get_orc_file_info')
    def test_process_file_batch(self, mock_get_info):
        """Test processing a batch of files."""
        # Mock the get_orc_file_info function
        mock_get_info.side_effect = [
            {
                'file_path': 's3://bucket/file1.orc',
                'file_length': 1024,
                'num_stripes': 3,
                'raw_data_size': 300
            },
            {
                'file_path': 's3://bucket/file2.orc',
                'file_length': 2048,
                'num_stripes': 5,
                'raw_data_size': 500
            }
        ]
        
        # Process a batch of files
        results = process_file_batch(['s3://bucket/file1.orc', 's3://bucket/file2.orc'], max_workers=2)
        
        # Verify the results
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['file_path'], 's3://bucket/file1.orc')
        self.assertEqual(results[1]['file_path'], 's3://bucket/file2.orc')


class TestCommandLineInterface(unittest.TestCase):
    
    @patch('orc_info_collector.process_file_batch')
    def test_main_function(self, mock_process_batch):
        """Test the main function with a sample input file."""
        # Mock the process_file_batch function
        mock_process_batch.return_value = [
            {
                'file_path': 's3://bucket/file1.orc',
                'file_length': 1024,
                'num_stripes': 3,
                'raw_data_size': 300
            },
            {
                'file_path': 's3://bucket/file2.orc',
                'file_length': 2048,
                'num_stripes': 5,
                'raw_data_size': 500
            }
        ]
        
        # Create a temporary input file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write('s3://bucket/file1.orc\n')
            f.write('s3://bucket/file2.orc\n')
            temp_filename = f.name
        
        try:
            # Run the main function with the temporary file
            with patch('sys.argv', ['orc_info_collector.py', temp_filename]):
                with patch('pandas.DataFrame.to_string', return_value='mocked_output'):
                    with patch('builtins.print') as mock_print:
                        from orc_info_collector import main
                        main()
                        
                        # Check that print was called with the DataFrame output
                        mock_print.assert_called_with('mocked_output')
        
        finally:
            # Clean up the temporary file
            os.unlink(temp_filename)


if __name__ == '__main__':
    unittest.main()