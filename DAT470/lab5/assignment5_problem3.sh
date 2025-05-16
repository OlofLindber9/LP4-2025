#!/bin/bash

source ~/miniforge3/bin/activate
conda activate pyspark

SCRIPT="assignment5_problem3.py"
DATASET="small"
SEED=0x9747b28c
NUM_REGISTERS=1024
FILEPATH="/data/courses/2025_dat470_dit066/gutenberg/$DATASET"

echo "Running $SCRIPT on the $DATASET dataset with seed $SEED,  $NUM_REGISTERS registers and $1 workers..."
python3 "$SCRIPT" "$FILEPATH" -s "$SEED" -m "$NUM_REGISTERS" -w $1