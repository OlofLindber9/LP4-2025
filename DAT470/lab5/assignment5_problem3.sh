#!/bin/bash

source ~/miniforge3/bin/activate
conda activate pyspark


# Positional arguments
NUM_WORKERS="64"
SEED="0x9747b28c"
NUM_REGISTERS="1024"

SCRIPT="assignment5_problem3c.py"
FILEPATH="/data/courses/2025_dat470_dit066/gutenberg/small"

echo "Running $SCRIPT with $NUM_WORKERS worker(s)..."
python3 "$SCRIPT" "$FILEPATH" -w "$NUM_WORKERS" -s "$SEED" -m "$NUM_REGISTERS"
