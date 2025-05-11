#!/bin/bash

source ~/miniforge3/bin/activate
conda activate pyspark

SCRIPT="assignment4_problem2.py"
FILEPATH="/data/courses/2025_dat470_dit066/climate/climate_large.csv"

echo "Running $SCRIPT with $1 worker(s)..."
python3 "$SCRIPT"  -w "$1" "$FILEPATH"