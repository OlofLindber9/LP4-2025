#!/bin/bash
#SBATCH --mem-per-cpu=256G

source ~/myenv/bin/activate

QUERY_SIZE=$1
BATCH_SIZE=$2

SCRIPT="assignment7_problem1.py"
# DATA="pubs/pub"
# DATA="glove/glove.6B.50d"
DATA="glove/glove.840B.300d"
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