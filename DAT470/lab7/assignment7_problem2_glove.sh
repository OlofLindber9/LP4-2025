#!/bin/bash
#SBATCH --gres=gpu:L40s:1
#SBATCH --mem-per-gpu=48G

source ~/miniforge3/bin/activate
conda activate rapids

QUERY_SIZE=$1
BATCH_SIZE=$2

SCRIPT="assignment7_problem2.py"
DATA="glove.6B.50d"
DATASET="/data/courses/2025_dat470_dit066/glove/$DATA.txt"
QUERIES="/data/courses/2025_dat470_dit066/glove/${DATA}_queries_${QUERY_SIZE}.txt"
LABELS="/data/courses/2025_dat470_dit066/glove/${DATA}_queries_${QUERY_SIZE}_names.txt"

echo "Running $SCRIPT on $DATA with $QUERY_SIZE queries, and batch size $BATCH_SIZE..."
echo "Node: $SLURM_JOB_NODELIST"
python3 "$SCRIPT" -d "$DATASET" -q "$QUERIES" -l "$LABELS" -b "$BATCH_SIZE"