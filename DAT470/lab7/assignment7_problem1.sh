#!/bin/bash
#SBATCH --mem-per-cpu=256G

BATCH_SIZE=$1
SIZE=$2

DATA="/pubs/pubs.csv"
DATASET="/data/courses/2025_dat470_dit066$DATA"
QUERIES="/data/courses/2025_dat470_dit066/pubs/pub_queries_${SIZE}.txt"
LABELS="/data/courses/2025_dat470_dit066/pubs/pub_queries_${SIZE}_names.txt"
SCRIPT="assignment7_problem1.py"

echo "Running $SCRIPT on $DATA with $SIZE queries, and batch size $BATCH_SIZE..."
echo "Node: $SLURM_JOB_NODELIST"
python3 "$SCRIPT" -d "$DATASET" -q "$QUERIES" -l "$LABELS" -b "$BATCH_SIZE"
