#!/bin/bash

DATA="/data/courses/2025_dat470_dit066/pubs/pubs.csv"
QUERIES="/data/courses/2025_dat470_dit066/pubs/pub_queries_small.txt"
SCRIPT="nnquery.py"
LABELS="/data/courses/2025_dat470_dit066/pubs/pub_queries_small_names.txt"

echo "Running $SCRIPT on $DATA with $QUERIES..."
python3 "$SCRIPT" -d "$DATA" -q "$QUERIES" -l "$LABELS"