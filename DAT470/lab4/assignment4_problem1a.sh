#!/bin/bash

source ~/miniforge3/bin/activate
conda activate pyspark

SCRIPT="assignment4_problem1a.py"
FILEPATH="/data/courses/2025_dat470_dit066/twitter/twitter-2010_10M.txt"

echo "Running $SCRIPT with $1 worker(s)..."
python3 "$SCRIPT"  -w "$1" "$FILEPATH"