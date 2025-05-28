#!/bin/bash
#SBATCH --gres=gpu:L40s:1
#SBATCH --mem-per-gpu=48G

source ~/miniforge3/bin/activate
conda activate rapids

QUERY_SIZE=$1
BATCH_SIZE=$2

SCRIPT="assignment7_problem4.py"
# DATA="glove/glove.6B.50d"
DATA="glove/glove.840B.300d"
# DATA="pubs/pub"
# If DATA is one of the glove sets, DATASET ends with .txt, otherwise it ends with .csv
if [[ "$DATA" == glove* ]]; then
    DATASET="/data/courses/2025_dat470_dit066/$DATA.txt"
else
    DATASET="/data/courses/2025_dat470_dit066/${DATA}s.csv"
fi
QUERIES="/data/courses/2025_dat470_dit066/${DATA}_queries_${QUERY_SIZE}.txt"
LABELS="/data/courses/2025_dat470_dit066/${DATA}_queries_${QUERY_SIZE}_names.txt"

echo "Running $SCRIPT on $DATA with $QUERY_SIZE queries, and batch size $BATCH_SIZE..."
echo "Node: $SLURM_JOB_NODELIST"
python3 "$SCRIPT" -d "$DATASET" -q "$QUERIES" -l "$LABELS" -b "$BATCH_SIZE"