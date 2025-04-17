#!/bin/bash

SCRIPT="assignment2_problem2f.py"

echo "Running $SCRIPT with $1 worker(s)..."
python3 "$SCRIPT"  -w "$1" /data/courses/2025_dat470_dit066/gutenberg/huge
