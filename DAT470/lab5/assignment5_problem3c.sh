#!/bin/bash

source ~/miniforge3/bin/activate
conda activate pyspark

SCRIPT="assignment5_problem3_eval.py"
FILEPATH="/data/courses/2025_dat470_dit066/gutenberg/$DATASET"
DATASET="small"
NUM_REGISTERS=1024

echo "Running $SCRIPT on the $DATASET datgaset and $NUM_REGISTERS registers..."
python3 "$SCRIPT" "$FILEPATH" -m "$NUM_REGISTERS" -w $1