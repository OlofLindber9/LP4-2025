#!/bin/bash

source ~/miniforge3/bin/activate
conda activate pyspark

SCRIPT="assignment5_problem3.py"
FILEPATH="/data/courses/2025_dat470_dit066/gutenberg/tiny"
SEED=0x9747b28c
NUM_REGISTERS=16

echo "Running $SCRIPT... with seed $SEED and $NUM_REGISTERS registers..."
python3 "$SCRIPT" "$FILEPATH" -s "$SEED" -m "$NUM_REGISTERS" 