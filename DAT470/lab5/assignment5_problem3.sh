#!/bin/bash

source ~/miniforge3/bin/activate
conda activate pyspark


# Positional arguments
NUM_WORKERS="32"
SEED="0x9747b28c"
NUM_REGISTERS="16"

SCRIPT="assignment5_problem3.py"
FILEPATH="/data/courses/2025_dat470_dit066/gutenberg/tiny"

echo "Running $SCRIPT with $NUM_WORKERS worker(s)..."
python3 "$SCRIPT" "$FILEPATH" -w "$NUM_WORKERS" -s "$SEED" -m "$NUM_REGISTERS"