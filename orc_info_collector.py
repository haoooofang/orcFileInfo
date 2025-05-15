#!/usr/bin/env python3
"""
ORC File Information Collector

This script collects information about ORC files stored on S3, including:
- Number of stripes
- File length

Usage:
    python orc_info_collector.py <input_file>

Where <input_file> contains a list of S3 paths to ORC files, one per line.
"""

import sys
import os
import argparse
import pyarrow.orc as orc
import pyarrow.fs as fs
import pandas as pd
from urllib.parse import urlparse
import concurrent.futures
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_s3_path(s3_path):
    """Parse an S3 path into bucket and key components."""
    parsed = urlparse(s3_path)
    if parsed.scheme != 's3':
        raise ValueError(f"Not an S3 path: {s3_path}")
    
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key


class S3ORCReader:
    """A class to read ORC file metadata from S3 efficiently using PyArrow's filesystem interface."""
    
    def __init__(self, s3_client=None):
        # Create PyArrow S3 filesystem
        self.s3_fs = fs.S3FileSystem(
            region=os.environ.get('AWS_REGION', 'us-west-2')
        )
    
    def get_orc_metadata(self, bucket, key):
        """
        Get ORC file metadata using PyArrow's filesystem interface.
        
        This method uses PyArrow's filesystem interface to:
        1. Read the ORC file metadata
        """
        # Construct the full S3 path
        s3_path = f"{bucket}/{key}"
        
        try:
            # Get file info using PyArrow's filesystem interface
            io_stream = self.s3_fs.open_input_file(s3_path)

            # Use PyArrow's ORC reader with the filesystem
            reader = orc.ORCFile(io_stream)
            
            # Get number of stripes
            num_stripes = reader.nstripes
            
            # Get file length
            file_length = reader.file_length
            
            return {
                'file_length': file_length,
                'num_stripes': num_stripes,
            }
        
        except Exception as e:
            logger.error(f"Error reading ORC metadata with PyArrow filesystem: {e}")
            return None


def get_orc_file_info(s3_path, s3_reader=None):
    """
    Get information about an ORC file without reading the entire file.
    
    Args:
        s3_path: S3 path to the ORC file
        s3_reader: Optional S3ORCReader instance
        
    Returns:
        dict: Information about the ORC file
    """
    try:
        # Parse S3 path
        bucket, key = parse_s3_path(s3_path)
        
        # Use provided reader or create a new one
        reader = s3_reader or S3ORCReader()
        
        # Get metadata
        metadata = reader.get_orc_metadata(bucket, key)
        
        # Return combined information
        return {
            'file_path': s3_path,
            'file_length': metadata['file_length'],
            'num_stripes': metadata['num_stripes'],
        }
    
    except Exception as e:
        logger.error(f"Error processing {s3_path}: {e}")
        return {
            'file_path': s3_path,
            'error': str(e)
        }


def process_file_batch(file_paths, max_workers=10):
    """Process a batch of files in parallel."""
    results = []
    
    # Create a shared S3 FS for all workers
    s3_reader = S3ORCReader()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_path = {
            executor.submit(get_orc_file_info, path, s3_reader): path 
            for path in file_paths
        }
        
        # Process results as they complete
        for i, future in enumerate(concurrent.futures.as_completed(future_to_path)):
            path = future_to_path[future]
            try:
                info = future.result()
                results.append(info)
                logger.info(f"Processed {i+1}/{len(file_paths)}: {path}")
            except Exception as e:
                logger.error(f"Error processing {path}: {e}")
                results.append({
                    'file_path': path,
                    'error': str(e)
                })
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Collect information about ORC files on S3')
    parser.add_argument('input_file', help='File containing a list of S3 paths to ORC files')
    parser.add_argument('--output', '-o', help='Output file path (default: stdout)')
    parser.add_argument('--workers', '-w', type=int, default=10, 
                        help='Number of worker threads (default: 10)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Read the list of ORC files
    with open(args.input_file, 'r') as f:
        orc_files = [line.strip() for line in f if line.strip()]
    
    logger.info(f"Processing {len(orc_files)} ORC files with {args.workers} workers")
    
    # Process files in parallel
    results = process_file_batch(orc_files, max_workers=args.workers)
    
    # Convert to DataFrame for easier handling
    df = pd.DataFrame(results)
    
    # Output results
    if args.output:
        df.to_csv(args.output, index=False)
        logger.info(f"Results saved to {args.output}")
    else:
        print(df.to_string())


if __name__ == "__main__":
    main()