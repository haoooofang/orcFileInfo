#!/usr/bin/env python3
"""
Example script to demonstrate how to use the ORC info collector with local files.
This is useful for testing without S3 access.

This script:
1. Creates a sample ORC file
2. Analyzes it using PyArrow's filesystem interface
3. Prints the results

Requirements:
- pyarrow
- pandas
"""

import os
import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.fs as fs
import pandas as pd
import tempfile


def create_sample_orc_file(path, num_rows=1000):
    """Create a sample ORC file with the specified number of rows."""
    # Create sample data
    data = {
        'id': list(range(num_rows)),
        'name': [f'name_{i}' for i in range(num_rows)],
        'value': [i * 1.5 for i in range(num_rows)]
    }
    
    # Convert to PyArrow Table
    table = pa.Table.from_pydict(data)
    
    # Write to ORC file with multiple stripes
    with open(path, 'wb') as f:
        # Use a small stripe size to create multiple stripes
        orc.write_table(table, f, stripe_size=1024 * 10)  # 10KB stripes
    
    print(f"Created sample ORC file at {path} with {num_rows} rows")


def analyze_local_orc_file(path):
    """Analyze a local ORC file and print information about it using PyArrow's filesystem interface."""
    # Create a local filesystem
    local_fs = fs.LocalFileSystem()
    
    # Get file info using PyArrow's filesystem interface
    file_info = local_fs.get_file_info(path)
    file_size = file_info.size
    
    # Open the ORC file using PyArrow's filesystem interface
    orc_file = orc.ORCFile(path, filesystem=local_fs)
    
    # Get number of stripes
    num_stripes = orc_file.nstripes
    
    # Get raw data size from ORC file metadata
    raw_data_size = get_raw_data_size(orc_file)
    
    # Print information
    print("\nORC File Information:")
    print(f"File path: {path}")
    print(f"File size (from PyArrow fs): {file_size} bytes")
    print(f"Number of stripes: {num_stripes}")
    print(f"Raw data size: {raw_data_size}")
    
    # Print stripe information
    print("\nStripe Information:")
    for i in range(num_stripes):
        stripe = orc_file.read_stripe(i)
        print(f"Stripe {i}: {len(stripe)} rows")
    
    # Read the schema
    print("\nSchema:")
    schema = orc_file.schema
    for i, field in enumerate(schema):
        print(f"  {i}: {field.name} ({field.type})")


def get_raw_data_size(reader):
    """
    Extract raw data size from ORC file metadata.
    
    The raw data size represents the uncompressed size of the data.
    """
    try:
        # Try to get content length from file metadata
        file_metadata = reader.metadata
        if hasattr(file_metadata, 'content_length'):
            return file_metadata.content_length
        
        # If content_length is not available, try to get it from statistics
        raw_data_size = 0
        for col_name, stats in reader.statistics.items():
            # If bytesOnDisk is available, it's a better estimate
            if 'bytesOnDisk' in stats:
                raw_data_size += stats['bytesOnDisk']
            elif 'numberOfValues' in stats:
                # Sum the total number of values as a fallback
                raw_data_size += stats['numberOfValues']
        
        return raw_data_size if raw_data_size > 0 else None
        
    except (KeyError, AttributeError) as e:
        print(f"Could not get raw data size: {e}")
        return None


def main():
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix='.orc', delete=False) as temp_file:
        temp_path = temp_file.name
    
    try:
        # Create a sample ORC file
        create_sample_orc_file(temp_path, num_rows=10000)
        
        # Analyze the file
        analyze_local_orc_file(temp_path)
    
    finally:
        # Clean up
        if os.path.exists(temp_path):
            os.unlink(temp_path)


if __name__ == "__main__":
    main()