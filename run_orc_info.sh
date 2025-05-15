#!/bin/bash
# Simple wrapper script for the ORC File Information Collector

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not found"
    exit 1
fi

# Check if the required packages are installed
python3 -c "import pyarrow, pandas, boto3" &> /dev/null
if [ $? -ne 0 ]; then
    echo "Installing required packages..."
    pip install -r requirements.txt
fi

# Check if input file is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <input_file> [--output <output_file>] [--workers <num_workers>] [--verbose]"
    echo ""
    echo "Example:"
    echo "  $0 my_orc_files.txt --output results.csv --workers 20 --verbose"
    exit 1
fi

# Run the script with all arguments passed through
python3 orc_info_collector.py "$@"