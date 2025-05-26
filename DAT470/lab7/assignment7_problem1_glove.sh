#!/bin/bash
#SBATCH -o original_glove_840B_medium.out

DATA="/data/courses/2025_dat470_dit066/glove/glove.840B.300d.txt"
QUERIES="/data/courses/2025_dat470_dit066/glove/glove.840B.300d_queries_medium.txt"
LABELS="/data/courses/2025_dat470_dit066/glove/glove.840B.300d_queries_medium_names.txt"
SCRIPT="assignment7_problem1.py"

echo "Running $SCRIPT on $DATA with $QUERIES..."
python3 "$SCRIPT" -d "$DATA" -q "$QUERIES" -l "$LABELS"