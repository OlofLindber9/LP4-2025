#!/bin/bash
#SBATCH -o pubs_medium.out

PUBS="/data/courses/2025_dat470_dit066/pubs/pubs.csv"
QUERIES="/data/courses/2025_dat470_dit066/pubs/pub_queries_medium.txt"
LABELS="/data/courses/2025_dat470_dit066/pubs/pub_queries_medium_names.txt"
SCRIPT="assignment7_problem1.py"

echo "Running $SCRIPT on $PUBS with $QUERIES..."
python3 "$SCRIPT" -d "$PUBS" -q "$QUERIES" -l "$LABELS" -b $1