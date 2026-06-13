#!/bin/bash
set -e
echo "Starting Call Detail Records (CDR) generation script..."
python3 /data/generate_records.py
echo "CDR generation completed successfully."
