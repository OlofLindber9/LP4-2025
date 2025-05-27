#!/bin/bash
#SBATCH --mem-per-cpu=256G

source ~/myenv/bin/activate

QUERY_SIZE=$1
# BATCH_SIZE=$2
BATCH_SIZE=95

SCRIPT="assignment7_problem1.py"
# DATA="glove.6B.50d"
DATA="glove.840B.300d"
DATASET="/data/courses/2025_dat470_dit066/glove/$DATA.txt"
QUERIES="/data/courses/2025_dat470_dit066/glove/${DATA}_queries_${QUERY_SIZE}.txt"
LABELS="/data/courses/2025_dat470_dit066/glove/${DATA}_queries_${QUERY_SIZE}_names.txt"

echo "Running $SCRIPT on $DATA with $QUERY_SIZE queries, and batch size $BATCH_SIZE..."
echo "Node: $SLURM_JOB_NODELIST"
python3 "$SCRIPT" -d "$DATASET" -q "$QUERIES" -l "$LABELS" -b "$BATCH_SIZE"