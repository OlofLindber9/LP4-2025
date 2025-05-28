#!/bin/bash
#SBATCH --gres=gpu:L40s:1
#SBATCH --mem-per-gpu=48G

DATA="/data/courses/2025_dat470_dit066/glove/glove.840B.300d.txt"
QUERIES="/data/courses/2025_dat470_dit066/glove/glove.840B.300d_queries_small.txt"
SCRIPT="assignment7_problem2.py"
LABELS="/data/courses/2025_dat470_dit066/glove/glove.840B.300d_queries_small_names.txt"
BATCHSIZE=5

echo "Running $SCRIPT on $DATA with $QUERIES and batchsize $BATCHSIZE..."
python3 "$SCRIPT" -d "$DATA" -q "$QUERIES" -l "$LABELS" -b "$BATCHSIZE"