# ORC File Information Collector

This tool efficiently collects information about ORC files stored on S3, including:
- Number of stripes
- File length (compressed size)
- Raw data size (uncompressed size, if available)

## Features

- **Efficient**: Reads only the necessary parts of ORC files (metadata and footer) without downloading entire files
- **Parallel Processing**: Processes multiple files concurrently for faster results
- **Minimal Dependencies**: Uses standard Python libraries and PyArrow for ORC file handling
- **PyArrow Filesystem Interface**: Uses PyArrow's filesystem interface to get file size and raw data size
- **Fallback Mechanism**: Falls back to legacy methods if PyArrow filesystem fails

## Requirements

- Python 3.6+
- Required packages:
  - pyarrow
  - pandas
  - boto3

Install dependencies:
```
pip install -r requirements.txt
```

## Usage

1. Create a text file containing a list of S3 paths to ORC files, one per line:
   ```
   s3://my-bucket/path/to/file1.orc
   s3://my-bucket/path/to/file2.orc
   s3://my-bucket/path/to/file3.orc
   ```

2. Run the script:
   ```
   python orc_info_collector.py input_file.txt
   ```

3. Additional options:
   ```
   python orc_info_collector.py input_file.txt --output results.csv  # Save to CSV
   python orc_info_collector.py input_file.txt --workers 20          # Use 20 worker threads
   python orc_info_collector.py input_file.txt --verbose             # Enable verbose logging
   ```

## How It Works

The script uses an optimized approach to extract metadata from ORC files:

1. **Metadata Extraction**: 
   - Uses PyArrow's filesystem interface to read ORC file metadata
   - Gets file size directly from the filesystem
   - Extracts raw data size from ORC file metadata
   - Falls back to reading only the file footer and postscript if PyArrow filesystem fails

2. **Parallel Processing**:
   - Uses thread pool to process multiple files concurrently
   - Shares S3 client across threads for efficiency

3. **Information Collected**:
   - File length from PyArrow's filesystem interface
   - Number of stripes from the ORC file metadata
   - Raw data size from ORC file metadata (content_length or bytesOnDisk)

## AWS Authentication

The script uses boto3 for S3 access, which will use your AWS credentials from:
- Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
- Shared credential file (~/.aws/credentials)
- EC2 instance profile or container role

Make sure you have appropriate permissions to access the S3 buckets containing the ORC files.

## Running Tests

To run the unit tests:
```
python -m unittest test_orc_info_collector.py
```